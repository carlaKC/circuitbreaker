package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/queue"
	"github.com/lightningnetwork/lnd/routing/route"
	migrate "github.com/rubenv/sql-migrate"
	_ "modernc.org/sqlite"
)

var migrations = &migrate.MemoryMigrationSource{
	Migrations: []*migrate.Migration{
		{
			Id: "1",
			Up: []string{
				`
				CREATE TABLE IF NOT EXISTS limits (
					peer TEXT PRIMARY KEY NOT NULL,
					htlc_max_pending INTEGER NOT NULL,
					htlc_max_hourly_rate INTEGER NOT NULL,
					mode TEXT CHECK(mode IN ('FAIL', 'QUEUE', 'QUEUE_PEER_INITIATED')) NOT NULL DEFAULT 'FAIL'
				);
				
				INSERT OR IGNORE INTO limits(peer, htlc_max_pending, htlc_max_hourly_rate) 
				VALUES('000000000000000000000000000000000000000000000000000000000000000000', 5, 3600);
				`,
			},
		},
		{
			Id: "2",
			Up: []string{
				`
				ALTER TABLE limits RENAME TO limits_old;

				CREATE TABLE IF NOT EXISTS limits (
					peer TEXT PRIMARY KEY NOT NULL,
					htlc_max_pending INTEGER NOT NULL,
					htlc_max_hourly_rate INTEGER NOT NULL,
					mode TEXT CHECK(mode IN ('FAIL', 'QUEUE', 'QUEUE_PEER_INITIATED', 'BLOCK')) NOT NULL DEFAULT 'FAIL'
				);

				INSERT INTO limits(peer, htlc_max_pending, htlc_max_hourly_rate, mode)
					SELECT peer, htlc_max_pending, htlc_max_hourly_rate, mode FROM limits_old;

				DROP TABLE limits_old;
				`,
			},
		},
		{
			Id: "3",
			Up: []string{
				`CREATE TABLE IF NOT EXISTS forwarding_history (
                                        id INTEGER PRIMARY KEY,
                                        add_time TIMESTAMP NOT NULL,
                                        resolved_time TIMESTAMP NOT NULL,
                                        settled BOOLEAN NOT NULL,
                                        incoming_amt_msat INTEGER NOT NULL,
                                        outgoing_amt_msat INTEGER NOT NULL,
                                        incoming_peer TEXT NOT NULL,
                                        incoming_channel INTEGER NOT NULL,
                                        incoming_htlc_index INTEGER NOT NULL,
                                        outgoing_peer TEXT NOT NULL,
                                        outgoing_channel INTEGER NOT NULL,
                                        outgoing_htlc_index INTEGER NOT NULL
                                );`,
			},
		},
	},
}

const (
	// defaultFwdHistoryLimit is the default limit we place on the forwarding_history table
	// to prevent creation of an ever-growing table.
	//
	// Justification for value:
	// * ~100 bytes per row in the table.
	// * Help ourselves to 10MB of disk space
	// -> 100_000 entries
	defaultFwdHistoryLimit = 100_000
)

var defaultNodeKey = route.Vertex{}

type Db struct {
	db *sql.DB

	fwdHistoryLimit int
}

func NewDb(dbPath string) (*Db, error) {
	const busyTimeoutMs = 5000

	dsn := dbPath + fmt.Sprintf("?_pragma=busy_timeout=%d", busyTimeoutMs)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}

	n, err := migrate.Exec(db, "sqlite3", migrations, migrate.Up)
	if err != nil {
		return nil, fmt.Errorf("migration error: %w", err)
	}
	if n > 0 {
		log.Infow("Applied migrations", "count", n)
	}

	return &Db{
		db:              db,
		fwdHistoryLimit: defaultFwdHistoryLimit,
	}, nil
}

func (d *Db) Close() error {
	return d.db.Close()
}

type Limit struct {
	MaxHourlyRate int64
	MaxPending    int64
	Mode          Mode
}

type Limits struct {
	Default Limit
	PerPeer map[route.Vertex]Limit
}

func (d *Db) UpdateLimit(ctx context.Context, peer route.Vertex,
	limit Limit) error {

	peerHex := hex.EncodeToString(peer[:])

	const replace string = `REPLACE INTO limits(peer, htlc_max_pending, htlc_max_hourly_rate, mode) VALUES(?, ?, ?, ?);`

	_, err := d.db.ExecContext(
		ctx, replace, peerHex,
		limit.MaxPending, limit.MaxHourlyRate,
		limit.Mode.String(),
	)

	return err
}

func (d *Db) ClearLimit(ctx context.Context, peer route.Vertex) error {
	if peer == defaultNodeKey {
		return errors.New("cannot clear default limit")
	}

	const query string = `DELETE FROM limits WHERE peer = ?;`

	_, err := d.db.ExecContext(
		ctx, query, hex.EncodeToString(peer[:]),
	)

	return err
}

func (d *Db) GetLimits(ctx context.Context) (*Limits, error) {
	const query string = `
	SELECT peer, htlc_max_pending, htlc_max_hourly_rate, mode from limits;`

	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	var limits = Limits{
		PerPeer: make(map[route.Vertex]Limit),
	}
	for rows.Next() {
		var (
			limit   Limit
			peerHex string
			modeStr string
		)
		err := rows.Scan(
			&peerHex, &limit.MaxPending, &limit.MaxHourlyRate, &modeStr,
		)
		if err != nil {
			return nil, err
		}

		switch modeStr {
		case "FAIL":
			limit.Mode = ModeFail

		case "QUEUE":
			limit.Mode = ModeQueue

		case "QUEUE_PEER_INITIATED":
			limit.Mode = ModeQueuePeerInitiated

		case "BLOCK":
			limit.Mode = ModeBlock

		case "LRC_ACTIVE":
			limit.Mode = ModeLRCActive

		case "LRC_LOGGING":
			limit.Mode = ModeLRCLogging
		default:
			return nil, errors.New("unknown mode")
		}

		key, err := route.NewVertexFromStr(peerHex)
		if err != nil {
			return nil, err
		}

		if key == defaultNodeKey {
			limits.Default = limit
		} else {
			limits.PerPeer[key] = limit
		}
	}

	return &limits, nil
}

type HtlcInfo struct {
	addTime         time.Time
	resolveTime     time.Time
	settled         bool
	incomingMsat    lnwire.MilliSatoshi
	outgoingMsat    lnwire.MilliSatoshi
	incomingPeer    route.Vertex
	outgoingPeer    route.Vertex
	incomingCircuit circuitKey
	outgoingCircuit circuitKey
}

// Less must return true if this item is ordered before other and false otherwise, using resolve
// time to sort by earliest resolution.
//
// Note: implements PriorityQueueItem interface.
func (h *HtlcInfo) Less(other queue.PriorityQueueItem) bool {
	return h.resolveTime.Before(other.(*HtlcInfo).resolveTime)
}

// RecordHtlcResolution records a HTLC that has been resolved and deletes the oldest rows from
// the forwarding history table if the total row count has exceeded the configured limit.
func (d *Db) RecordHtlcResolution(ctx context.Context,
	htlc *HtlcInfo) error {

	if err := d.insertHtlcResolution(ctx, htlc); err != nil {
		return err
	}

	return d.limitHTLCRecords(ctx)
}

func (d *Db) insertHtlcResolution(ctx context.Context, htlc *HtlcInfo) error {
	insert := `INSERT INTO forwarding_history (
                add_time,
                resolved_time,
                settled,
                incoming_amt_msat,
                outgoing_amt_msat,
                incoming_peer,
                incoming_channel,
                incoming_htlc_index,
                outgoing_peer,
                outgoing_channel,
                outgoing_htlc_index)
                VALUES (?,?,?,?,?,?,?,?,?,?,?);`

	_, err := d.db.ExecContext(
		ctx, insert,
		htlc.addTime.UnixNano(),
		htlc.resolveTime.UnixNano(),
		htlc.settled,
		uint64(htlc.incomingMsat),
		uint64(htlc.outgoingMsat),
		hex.EncodeToString(htlc.incomingPeer[:]),
		htlc.incomingCircuit.channel,
		htlc.incomingCircuit.htlc,
		hex.EncodeToString(htlc.outgoingPeer[:]),
		htlc.outgoingCircuit.channel,
		htlc.outgoingCircuit.htlc,
	)

	return err
}

func (d *Db) limitHTLCRecords(ctx context.Context) error {
	query := `SELECT COUNT(id) from forwarding_history`

	var rowCount int
	err := d.db.QueryRow(query).Scan(&rowCount)
	if err != nil {
		return err
	}

	if rowCount < d.fwdHistoryLimit {
		return nil
	}

	// If we've hit our row count, delete oldest entries over the row limit plus an extra
	// 10% of the limit to free up space so that we don't need to constantly delete on each
	// insert.
	//
	// Note: if fwdHistoryLimit < 10 the additional 10% will be zero, so we'll just clear the
	// rows beyond our limit. For such a small limit, we're expecting to be deleting all the
	// time anyway, so this isn't a big performance hit.
	offset := d.fwdHistoryLimit - (d.fwdHistoryLimit / 10)

	query = fmt.Sprintf(
		`DELETE FROM forwarding_history 
                ORDER BY add_time 
                DESC OFFSET %v;`,
		offset,
	)

	_, err = d.db.ExecContext(ctx, query)

	return err
}

// ListForwardingHistory returns a list of htlcs that were resolved within the
// time range provided (start time is inclusive, end time is exclusive)
func (d *Db) ListForwardingHistory(ctx context.Context, start, end time.Time) (
	[]*HtlcInfo, error) {

	return d.listforwardingHistoryWhere(ctx, "add_time >= ? AND add_time < ?",
		start.UnixNano(), end.UnixNano())
}

// listForwardingHistoryWhere obtains a list of HTLCs from the forwarding history table,
// appending a caller-provided where clause to the standard query.
func (d *Db) listforwardingHistoryWhere(ctx context.Context, whereClause string,
	args ...any) ([]*HtlcInfo, error) {

	list := fmt.Sprintf(`SELECT 
                add_time,
                resolved_time,
                settled,
                incoming_amt_msat,
                outgoing_amt_msat,
                incoming_peer,
                incoming_channel,
                incoming_htlc_index,
                outgoing_peer,
                outgoing_channel,
                outgoing_htlc_index 
        FROM forwarding_history 
        WHERE %v;`, whereClause)

	rows, err := d.db.QueryContext(ctx, list, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var htlcs []*HtlcInfo
	for rows.Next() {
		var (
			incomingPeer, outgoingPeer string
			addTime, resolveTime       uint64
			htlc                       HtlcInfo
		)

		err := rows.Scan(
			&addTime,
			&resolveTime,
			&htlc.settled,
			&htlc.incomingMsat,
			&htlc.outgoingMsat,
			&incomingPeer,
			&htlc.incomingCircuit.channel,
			&htlc.incomingCircuit.htlc,
			&outgoingPeer,
			&htlc.outgoingCircuit.channel,
			&htlc.outgoingCircuit.htlc,
		)
		if err != nil {
			return nil, err
		}
		htlc.addTime = time.Unix(0, int64(addTime))
		htlc.resolveTime = time.Unix(0, int64(resolveTime))

		htlc.incomingPeer, err = route.NewVertexFromStr(incomingPeer)
		if err != nil {
			return nil, err
		}

		htlc.outgoingPeer, err = route.NewVertexFromStr(outgoingPeer)
		if err != nil {
			return nil, err
		}

		htlcs = append(htlcs, &htlc)
	}

	return htlcs, nil
}

// ListInFlightAt returns all the HTLCs that were in flight at the timestamp provided. A HTLC is
// considered in-flight at a given time if:
// - It was added before (or at) the timestamp.
// - It was removed after the timestamp.
func (d *Db) ListInFlightAt(ctx context.Context, ts time.Time) ([]*HtlcInfo, error) {
	nano := ts.UnixNano()
	return d.listforwardingHistoryWhere(ctx, "add_time <= ? AND resolved_time >= ?", nano, nano)
}
