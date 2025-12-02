-- migration metadata and helpers for p2i migrations
CREATE TABLE IF NOT EXISTS migration_history (
  id BIGSERIAL PRIMARY KEY,
  batch_id TEXT NOT NULL,
  relname TEXT NOT NULL,
  cutoff_ts TIMESTAMPTZ NOT NULL,
  iceberg_snapshot_id TEXT,
  s3_path TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  status TEXT CHECK (status IN ('queued','processing','success','failed')) NOT NULL DEFAULT 'queued',
  notes TEXT,
  UNIQUE (batch_id, relname)
);

CREATE TABLE IF NOT EXISTS migration_outbox (
  id BIGSERIAL PRIMARY KEY,
  batch_id TEXT NOT NULL,
  relname TEXT NOT NULL,
  cutoff_ts TIMESTAMPTZ NOT NULL,
  state TEXT CHECK (state IN ('queued','processing','committed','failed')) NOT NULL DEFAULT 'queued',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error TEXT
);

CREATE OR REPLACE FUNCTION prevent_updates_on_sealed_rows()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF NEW.migrated IS DISTINCT FROM OLD.migrated AND OLD.migrated = TRUE THEN
      RAISE EXCEPTION 'Cannot update migrated row';
    END IF;
    IF OLD.sealed_at IS NOT NULL THEN
      RAISE EXCEPTION 'Cannot update sealed row';
    END IF;
  ELSIF TG_OP = 'DELETE' THEN
    IF OLD.migrated = TRUE OR OLD.sealed_at IS NOT NULL THEN
      RAISE EXCEPTION 'Cannot delete migrated/sealed row';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;