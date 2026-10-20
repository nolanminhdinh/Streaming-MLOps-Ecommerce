-- Additive migration for existing installations. No facts are rewritten.
ALTER TABLE Fact_Orders ADD COLUMN IF NOT EXISTS source_updated_at TIMESTAMP;
ALTER TABLE Fact_Inventory_Daily ADD COLUMN IF NOT EXISTS snapshot_at TIMESTAMPTZ;
