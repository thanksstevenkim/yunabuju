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
    matched_description TEXT,  -- 개인 서버 판단 근거 저장
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    discovery_batch_id VARCHAR(50),
    discovery_status VARCHAR(20) DEFAULT 'pending',
    discovery_started_at TIMESTAMP,
    discovery_completed_at TIMESTAMP,
    suspicious_reason VARCHAR(50),
    suspicious_details TEXT,
    blocked_until TIMESTAMP,
    last_response_size BIGINT,
    last_content_type VARCHAR(100),
    node_name VARCHAR(255),
    node_description TEXT,
    registration_type VARCHAR(20) DEFAULT 'open' -- 'open', 'approval_required', 'closed'
  );
`;

const createDiscoveryStateTableSQL = `
  CREATE TABLE IF NOT EXISTS yunabuju_discovery_state (
    id INTEGER PRIMARY KEY DEFAULT 1,
    current_depth INTEGER DEFAULT 0,
    processed_servers TEXT[] DEFAULT '{}',
    new_servers TEXT[] DEFAULT '{}',
    last_processed_index INTEGER DEFAULT 0,
    current_peers TEXT[] DEFAULT '{}',
    current_server VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT single_row CHECK (id = 1)
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

    console.log("Creating table schemas if not exists...");
    await client.query(createTableSQL);
    await client.query(createDiscoveryStateTableSQL);

    // Add new columns if they don't exist
    const alterTableSQL = `
      DO $$ 
      BEGIN 
        BEGIN
          ALTER TABLE yunabuju_discovery_state 
            ADD COLUMN current_peers TEXT[] DEFAULT '{}';
        EXCEPTION
          WHEN duplicate_column THEN NULL;
        END;
        
        BEGIN
          ALTER TABLE yunabuju_discovery_state 
            ADD COLUMN current_server VARCHAR(255);
        EXCEPTION
          WHEN duplicate_column THEN NULL;
        END;

        BEGIN
          ALTER TABLE yunabuju_servers
            ADD COLUMN suspicious_reason VARCHAR(50);
        EXCEPTION
          WHEN duplicate_column THEN NULL;
        END;

        BEGIN
          ALTER TABLE yunabuju_servers
            ADD COLUMN suspicious_details TEXT;
        EXCEPTION
          WHEN duplicate_column THEN NULL;
        END;

        BEGIN
          ALTER TABLE yunabuju_servers
            ADD COLUMN blocked_until TIMESTAMP;
        EXCEPTION
          WHEN duplicate_column THEN NULL;
        END;

        BEGIN
          ALTER TABLE yunabuju_servers
            ADD COLUMN last_response_size BIGINT;
        EXCEPTION
          WHEN duplicate_column THEN NULL;
        END;

        BEGIN
          ALTER TABLE yunabuju_servers
            ADD COLUMN last_content_type VARCHAR(100);
        EXCEPTION
          WHEN duplicate_column THEN NULL;
        END;

        BEGIN
          ALTER TABLE yunabuju_servers
            ADD COLUMN node_name VARCHAR(255);
        EXCEPTION
          WHEN duplicate_column THEN NULL;
        END;

        BEGIN
          ALTER TABLE yunabuju_servers
            ADD COLUMN node_description TEXT;
        EXCEPTION
          WHEN duplicate_column THEN NULL;
        END;

        BEGIN
          ALTER TABLE yunabuju_servers
            ADD COLUMN registration_type VARCHAR(20) DEFAULT 'open';
        EXCEPTION
          WHEN duplicate_column THEN NULL;
        END;
      END $$;
    `;
    await client.query(alterTableSQL);

    console.log("Database setup completed successfully");

    client.release();
  } catch (error) {
    console.error("Error setting up database:", error);
    throw error;
  } finally {
    await pool.end();
  }
}

// 데이터베이스 초기화 함수 추가
async function resetDatabase() {
  console.log("Starting database reset...");

  const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL,
  });

  try {
    console.log("Connecting to database...");
    const client = await pool.connect();

    console.log("Dropping existing tables...");
    await client.query(`
      DROP TABLE IF EXISTS yunabuju_servers CASCADE;
      DROP TABLE IF EXISTS yunabuju_discovery_state CASCADE;
    `);

    console.log("Creating tables from scratch...");
    await client.query(createTableSQL);
    await client.query(createDiscoveryStateTableSQL);

    console.log("Database reset completed successfully");
    client.release();
  } catch (error) {
    console.error("Error resetting database:", error);
    throw error;
  } finally {
    await pool.end();
  }
}

const currentFile = fileURLToPath(import.meta.url);

if (process.argv[1] === currentFile) {
  const command = process.argv[2];

  if (command === "reset") {
    console.log("Resetting database...");
    resetDatabase()
      .then(() => {
        console.log("Database reset completed");
        process.exit(0);
      })
      .catch((error) => {
        console.error("Database reset failed:", error);
        process.exit(1);
      });
  } else {
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
}

export { setupDatabase, resetDatabase };
