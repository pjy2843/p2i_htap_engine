-- Create triggers on target tables to prevent updates/deletes after migration/sealing
-- Adjust schema prefixes if your tables are not in public schema

CREATE TRIGGER trg_prevent_updates_orders
BEFORE UPDATE OR DELETE ON orders
FOR EACH ROW
EXECUTE FUNCTION prevent_updates_on_sealed_rows();

CREATE TRIGGER trg_prevent_updates_order_line
BEFORE UPDATE OR DELETE ON order_line
FOR EACH ROW
EXECUTE FUNCTION prevent_updates_on_sealed_rows();

-- hhistory is append-only; no trigger is necessary unless you want to prevent deletes explicitly
-- CREATE TRIGGER trg_prevent_updates_hhistory
-- BEFORE UPDATE OR DELETE ON hhistory
-- FOR EACH ROW
-- EXECUTE FUNCTION prevent_updates_on_sealed_rows();
