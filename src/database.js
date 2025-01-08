import pg from "pg";
import dotenv from "dotenv";

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
  const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL,
  });

  try {
    // 기존 테이블 삭제
    await pool.query("DROP TABLE IF EXISTS korean_servers;");

    // 새 테이블 생성
    await pool.query(createTableSQL);
    console.log("Database table created successfully");
  } catch (error) {
    console.error("Error creating database table:", error);
  } finally {
    await pool.end();
  }
}

// 직접 실행된 경우에만 setupDatabase 실행
if (process.argv[1] === new URL(import.meta.url).pathname) {
  setupDatabase();
}
