import pg from "pg";
import dotenv from "dotenv";
import { fileURLToPath } from "url";

dotenv.config();

const createTableSQL = `
  CREATE TABLE IF NOT EXISTS yunabuju_servers (
    id SERIAL PRIMARY KEY,
    domain VARCHAR(255) NOT NULL UNIQUE,
    first_discovered TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_checked TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    korean_usage_rate FLOAT,
    description TEXT,
    total_users INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
  );
`;

const alterTableSQL = `
  DO $$ 
  BEGIN
    -- Add software_name if not exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
      WHERE table_name = 'yunabuju_servers' AND column_name = 'software_name') THEN
      ALTER TABLE yunabuju_servers ADD COLUMN software_name VARCHAR(50);
    END IF;

    -- Add software_version if not exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
      WHERE table_name = 'yunabuju_servers' AND column_name = 'software_version') THEN
      ALTER TABLE yunabuju_servers ADD COLUMN software_version VARCHAR(50);
    END IF;

    -- Add registration_open if not exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
      WHERE table_name = 'yunabuju_servers' AND column_name = 'registration_open') THEN
      ALTER TABLE yunabuju_servers ADD COLUMN registration_open BOOLEAN DEFAULT NULL;
    END IF;

    -- Add registration_approval_required if not exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
      WHERE table_name = 'yunabuju_servers' AND column_name = 'registration_approval_required') THEN
      ALTER TABLE yunabuju_servers ADD COLUMN registration_approval_required BOOLEAN DEFAULT NULL;
    END IF;
  END $$;
`;

async function setupDatabase() {
  console.log("Starting database setup...");

  const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL,
  });

  try {
    console.log("Connecting to database...");
    const client = await pool.connect();

    console.log("Creating table if not exists...");
    await client.query(createTableSQL);
    console.log("Database table checked successfully");

    console.log("Adding new columns if not exist...");
    await client.query(alterTableSQL);
    console.log("Database columns updated successfully");

    client.release();
  } catch (error) {
    console.error("Error setting up database:", error);
    throw error;
  } finally {
    await pool.end();
  }
}

const currentFile = fileURLToPath(import.meta.url);

// 직접 실행된 경우에만 setupDatabase 실행
if (process.argv[1] === currentFile) {
  console.log("Running database setup directly...");
  setupDatabase()
    .then(() => {
      console.log("Database setup completed successfully");
      process.exit(0);
    })
    .catch((error) => {
      console.error("Database setup failed:", error);
      process.exit(1);
    });
} else {
  console.log("Module imported, not running setup");
}

export { setupDatabase };
