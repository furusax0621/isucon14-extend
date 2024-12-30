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
	if err := db.SelectContext(ctx, &chairs, `SELECT chairs.*, cl.latitude, cl.longitude FROM chairs JOIN chair_last_locations AS cl ON cl.chair_id = chairs.id WHERE is_free = TRUE AND is_active = TRUE`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	matchedList := make(map[string]string)
	rideMap := make(map[string]Ride)

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

		// マッチングした椅子がなければ次のライドへ
		if matched.ID == "" {
			continue
		}
		matchedList[ride.ID] = matched.ID
		rideMap[ride.ID] = ride

		// 割り当てた椅子を割り当て待ちから削除
		chairs = slices.Delete(chairs, matchedIndex, matchedIndex+1)
	}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	rideMapByChairIDMutex.Lock()
	defer rideMapByChairIDMutex.Unlock()
	for rideID, chairID := range matchedList {
		if _, err := tx.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", chairID, rideID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if _, err := tx.ExecContext(ctx, "UPDATE chairs SET is_free = FALSE WHERE id = ?", chairID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		rideMapByChairID[chairID] = rideMap[rideID]
	}
	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
