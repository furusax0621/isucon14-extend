package main

import (
	"net/http"
	"slices"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// まだ椅子が割り当てられていないライドを取得。ループの都合上、より時間が経過しているものから取得
	rides := []Ride{}
	if err := db.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at ASC`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 割当待ちのライドがなければ一件落着
	if len(rides) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 現在空いている椅子を取得
	chairs := []ChairWithLocation{}
	if err := db.SelectContext(ctx, &chairs, `SELECT * FROM chairs WHERE is_free = TRUE AND is_active = TRUE`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// ライドと椅子をマッチング
	for _, ride := range rides {
		distance := 400
		var matched ChairWithLocation
		var matchedIndex int
		for i, chair := range chairs {
			d := calculateDistance(ride.PickupLatitude, ride.PickupLongitude, chair.Latitude, chair.Longitude)
			if d < distance {
				distance = d
				matched = chair
				matchedIndex = i
			}
		}

		// マッチングした椅子をライドに割り当て
		err := func() error {
			tx, err := db.Beginx()
			if err != nil {
				return err
			}
			defer tx.Rollback()

			if _, err := tx.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matched.ID, ride.ID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, "UPDATE chairs SET is_free = FALSE WHERE id = ?", matched.ID); err != nil {
				return err
			}
			if err := tx.Commit(); err != nil {
				return err
			}

			return nil
		}()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		// 割り当てた椅子を割り当て待ちから削除
		chairs = slices.Delete(chairs, matchedIndex, 1)
	}

	w.WriteHeader(http.StatusNoContent)
}
