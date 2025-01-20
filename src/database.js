// database.js
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
    software_name VARCHAR(50),
    software_version VARCHAR(50),
    registration_open BOOLEAN DEFAULT NULL,
    registration_approval_required BOOLEAN DEFAULT NULL,
    has_nodeinfo BOOLEAN DEFAULT NULL,
    failed_attempts INTEGER DEFAULT 0,
    last_failed_at TIMESTAMP,
    next_check_at TIMESTAMP,
    is_korean_server BOOLEAN DEFAULT NULL,
    last_korean_check TIMESTAMP,
    next_korean_check TIMESTAMP,
    is_personal_instance BOOLEAN DEFAULT NULL,
    instance_type VARCHAR(20) DEFAULT 'unknown',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
  );
`;

async function setupDatabase() {
  console.log("Starting database setup...");

  const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL,
  });

  try {
    console.log("Connecting to database...");
    const client = await pool.connect();

    console.log("Creating/updating table schema...");
    await client.query(createTableSQL);

    // Run migration to add new columns
    const migrationSQL = `
      ALTER TABLE yunabuju_servers
      ADD COLUMN IF NOT EXISTS software_name VARCHAR(50),
      ADD COLUMN IF NOT EXISTS software_version VARCHAR(50),
      ADD COLUMN IF NOT EXISTS registration_open BOOLEAN DEFAULT NULL,
      ADD COLUMN IF NOT EXISTS registration_approval_required BOOLEAN DEFAULT NULL,
      ADD COLUMN IF NOT EXISTS has_nodeinfo BOOLEAN DEFAULT NULL,
      ADD COLUMN IF NOT EXISTS failed_attempts INTEGER DEFAULT 0,
      ADD COLUMN IF NOT EXISTS last_failed_at TIMESTAMP,
      ADD COLUMN IF NOT EXISTS next_check_at TIMESTAMP,
      ADD COLUMN IF NOT EXISTS is_korean_server BOOLEAN DEFAULT NULL,
      ADD COLUMN IF NOT EXISTS last_korean_check TIMESTAMP,
      ADD COLUMN IF NOT EXISTS next_korean_check TIMESTAMP,
      ADD COLUMN IF NOT EXISTS is_personal_instance BOOLEAN DEFAULT NULL,
      ADD COLUMN IF NOT EXISTS instance_type VARCHAR(20) DEFAULT 'unknown';
    `;

    await client.query(migrationSQL);
    console.log("Database setup completed successfully");

    client.release();
  } catch (error) {
    console.error("Error setting up database:", error);
    throw error;
  } finally {
    await pool.end();
  }
}

const currentFile = fileURLToPath(import.meta.url);

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
}

export { setupDatabase };
