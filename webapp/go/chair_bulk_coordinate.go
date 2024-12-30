package main

import (
	"context"
	"log/slog"
	"strings"
	"time"
)

var chairLocationQueue = make(chan ChairLocation, 10000)

func bulkInsertChairLocation() {
	ctx := context.Background()
LOOP1:
	for {
		chairLocations := make([]ChairLocation, 0, 10000)

	LOOP2:
		for {
			ticker := time.NewTicker(1 * time.Second)

			select {
			case <-ticker.C:
				break LOOP2

			case chairLocation := <-chairLocationQueue:
				chairLocations = append(chairLocations, chairLocation)

				// バルクインサートする最大件数を超えたら即座にインサート
				if len(chairLocations) >= 10000 {
					break LOOP2
				}
			}
		}

		// バルクインサートするデータがない場合はスキップ
		if len(chairLocations) == 0 {
			continue LOOP1
		}

		// バルクインサート
		// MySQL 的には一回のクエリで 65,535 個までプレースホルダーを設定できる
		values := make([]interface{}, 0, len(chairLocations)*5)
		for _, l := range chairLocations {
			values = append(values, l.ID, l.ChairID, l.Latitude, l.Longitude, l.CreatedAt)
		}
		placeholders := strings.Repeat("(?, ?, ?, ?, ?),", len(chairLocations))
		placeholders = placeholders[:len(placeholders)-1]

		err := func() error {
			tx, err := db.Beginx()
			if err != nil {
				return err
			}
			defer tx.Rollback()

			if _, err := tx.ExecContext(
				ctx,
				"INSERT INTO chair_locations (id, chair_id, latitude, longitude, created_at) VALUES "+placeholders,
				values...,
			); err != nil {
				return err
			}

			// UPSERT のバルク化がよくわからなかったの :(
			stmt, err := tx.PreparexContext(
				ctx,
				"INSERT INTO chair_last_locations (chair_id, latitude, longitude, updated_at, total_distance) VALUES (?, ?, ?, ?, 0) AS new "+
					"ON DUPLICATE KEY UPDATE "+
					"total_distance = chair_last_locations.total_distance + ABS(chair_last_locations.latitude - new.latitude) + ABS(chair_last_locations.longitude - new.longitude), "+
					"latitude = new.latitude, longitude = new.longitude, updated_at = new.updated_at ",
			)
			if err != nil {
				return err
			}
			defer stmt.Close()

			for _, l := range chairLocations {
				if _, err := stmt.ExecContext(ctx, l.ChairID, l.Latitude, l.Longitude, l.CreatedAt); err != nil {
					return err
				}
			}

			if err := tx.Commit(); err != nil {
				return err
			}

			return nil
		}()
		if err != nil {
			slog.Error("failed to insert chair locations", slog.Any("error", err))
		}
	}
}
