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

async function setupDatabase() {
  console.log("Starting database setup...");

  const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL,
  });

  try {
    console.log("Connecting to database...");
    const client = await pool.connect();

    console.log("Dropping old table if exists...");
    await client.query("DROP TABLE IF EXISTS korean_servers;");

    console.log("Creating new table...");
    await client.query(createTableSQL);
    console.log("Database table created successfully");

    client.release();
  } catch (error) {
    console.error("Error creating database table:", error);
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
