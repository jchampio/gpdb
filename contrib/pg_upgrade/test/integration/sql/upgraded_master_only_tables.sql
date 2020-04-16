-- The following tables should contain no entries on segments.
SELECT gp_segment_id, * FROM gp_dist_random('gp_configuration_history');
SELECT gp_segment_id, * FROM gp_dist_random('gp_segment_configuration');
SELECT gp_segment_id, * FROM gp_dist_random('pg_partition');
SELECT gp_segment_id, * FROM gp_dist_random('pg_partition_encoding');
SELECT gp_segment_id, * FROM gp_dist_random('pg_partition_rule');
SELECT gp_segment_id, * FROM gp_dist_random('pg_stat_last_operation');
SELECT gp_segment_id, * FROM gp_dist_random('pg_stat_last_shoperation');
