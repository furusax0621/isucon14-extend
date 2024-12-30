ALTER TABLE chair_locations ADD INDEX chair_id_idx (chair_id, created_at DESC);
ALTER TABLE ride_statuses ADD INDEX ride_id_idx (ride_id, created_at DESC);
ALTER TABLE rides ADD INDEX chair_id_idx (chair_id, updated_at DESC);
ALTER TABLE rides ADD INDEX user_id_idx (user_id, created_at DESC);
ALTER TABLE chairs ADD INDEX owner_id_idx (owner_id);
ALTER TABLE chairs ADD INDEX access_token_idx (access_token);
ALTER TABLE coupons ADD INDEX used_by_idx (used_by);
ALTER TABLE coupons ADD INDEX code_idx (code);

ALTER TABLE chairs ADD COLUMN is_free TINYINT(1) NOT NULL DEFAULT 1 COMMENT '空き状態';
