--
-- This test exists to ensure that "master-only tables" (defined here as tables
-- that should have data only on the master and not on any segments) are
-- correctly handled by pg_upgrade. Some of these tables have contents without
-- any effort on our part; for everything else, add code here to populate them.
--

-- To populate pg_partition/_rule/_encoding, create a subpartition template with
-- AO/CO compression.
CREATE TABLE subpartition_encoding (a int, b int, c int)
    WITH (appendonly=true, orientation=column)
    PARTITION BY RANGE(a)
	    SUBPARTITION BY LIST(b)
		SUBPARTITION TEMPLATE(
		    SUBPARTITION subpart VALUES(1),
			COLUMN a ENCODING(compresstype=zlib),
			COLUMN b ENCODING(compresstype=zlib),
			COLUMN c ENCODING(compresstype=zlib)
		)
	(PARTITION part START(1) END(10));
