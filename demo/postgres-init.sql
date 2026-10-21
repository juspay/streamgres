-- The publication the engine's change feed streams. FOR ALL TABLES
-- needs a superuser, which the compose file's POSTGRES_USER is.
CREATE PUBLICATION demo_pub FOR ALL TABLES;
