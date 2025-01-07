// server.js
import express from "express";
import cors from "cors";
import pg from "pg";
import { KoreanActivityPubDiscovery } from "./discovery.js";
import cron from "node-cron";
import winston from "winston";
import dotenv from "dotenv";

dotenv.config();

// 로거 설정
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: "error.log", level: "error" }),
    new winston.transports.File({ filename: "combined.log" }),
  ],
});

// DB 설정
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
});

const app = express();
app.use(cors());
app.use(express.json());

// 디스커버리 인스턴스 생성
const discovery = new KoreanActivityPubDiscovery(pool, logger);

// API 라우트
app.get("/yunabuju/servers", async (req, res) => {
  try {
    const servers = await discovery.getKnownServers();
    res.json(servers);
  } catch (error) {
    logger.error("Error fetching servers:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// 매일 새벽 3시에 서버 검색 실행 (한국 시간)
cron.schedule(
  "0 3 * * *",
  async () => {
    try {
      await discovery.startDiscovery();
      logger.info("Daily server discovery completed");
    } catch (error) {
      logger.error("Error in daily server discovery:", error);
    }
  },
  {
    timezone: "Asia/Seoul",
  }
);

const PORT = process.env.PORT || 3500;
app.listen(PORT, () => {
  logger.info(`Server is running on port ${PORT}`);
});
