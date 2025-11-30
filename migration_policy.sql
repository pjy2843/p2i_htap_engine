ALTER TABLE sync_policy
  ADD COLUMN IF NOT EXISTS enabled boolean NOT NULL DEFAULT true;

-- (선택) category 컬럼이 없거나 제약이 없으면 보강
ALTER TABLE sync_policy
  ADD COLUMN IF NOT EXISTS category text;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'sync_policy'::regclass
      AND conname = 'sync_policy_category_check'
  ) THEN
    ALTER TABLE sync_policy
      ADD CONSTRAINT sync_policy_category_check
      CHECK (category IN ('append_only','update_once'));
  END IF;
END$$;
