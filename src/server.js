import express from "express";
import cors from "cors";
import pg from "pg";
import { KoreanActivityPubDiscovery } from "./discovery.js";
import cron from "node-cron";
import winston from "winston";
import dotenv from "dotenv";
import { setupDatabase } from "./database.js";

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

if (process.env.NODE_ENV !== "production") {
  logger.add(
    new winston.transports.Console({
      format: winston.format.simple(),
    })
  );
}

// DB 설정
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
});

async function initializeServer() {
  try {
    // 데이터베이스 테이블 설정
    await setupDatabase();
    logger.info("Database setup completed");

    return true;
  } catch (error) {
    logger.error("Error during server initialization:", error);
    throw error;
  }
}

async function startServer() {
  try {
    // 서버 초기화
    await initializeServer();

    const app = express();
    app.use(cors());
    app.use(express.json());

    // 디스커버리 인스턴스 생성
    const discovery = new KoreanActivityPubDiscovery(pool, logger);

    // 루트 경로에 상태 및 기본 정보 제공
    app.get("/", (req, res) => {
      res.json({
        name: "Yunabuju - Korean ActivityPub Directory",
        status: "running",
        endpoints: {
          servers: "/yunabuju/servers",
          serversAll: "/yunabuju/servers/all",
          discover: "/yunabuju/discover",
          discoverStatus: "/yunabuju/discover/status",
          status: "/yunabuju/status",
        },
        version: "1.0.0",
      });
    });

    // API 라우트 - 기본 서버 목록 (가입 가능한 커뮤니티 서버만)
    app.get("/yunabuju/servers", async (req, res) => {
      try {
        const includeClosedRegistration =
          req.query.includeClosedRegistration === "true";
        // 기본적으로 개인 인스턴스는 제외
        const servers = await discovery.getKnownServers(
          includeClosedRegistration,
          true
        );
        res.json({
          total: servers.length,
          registration_filtered: !includeClosedRegistration,
          personal_instances_filtered: true,
          servers: servers,
        });
      } catch (error) {
        logger.error("Error fetching servers:", error);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // API 라우트 - 모든 서버 목록 (관리자용)
    app.get("/yunabuju/servers/all", async (req, res) => {
      try {
        const includePersonal = req.query.includePersonal === "true";
        const servers = await discovery.getKnownServers(true, !includePersonal);
        res.json({
          total: servers.length,
          registration_filtered: false,
          personal_instances_filtered: !includePersonal,
          servers: servers,
        });
      } catch (error) {
        logger.error("Error fetching all servers:", error);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // 수동으로 서버 검색을 시작하는 엔드포인트
    app.post("/yunabuju/discover", async (req, res) => {
      try {
        const { resume } = req.query;
        logger.info(
          `${resume ? "Resuming" : "Starting"} manual server discovery...`
        );

        if (resume === "true") {
          const result = await discovery.resumeDiscovery();
          if (!result) {
            return res.json({ message: "No unfinished batch found to resume" });
          }
        } else {
          await discovery.startDiscovery();
        }

        const servers = await discovery.getKnownServers(true, true); // 개인 인스턴스 제외
        logger.info(
          `Discovery ${resume ? "resumed and" : ""} completed. Found ${
            servers.length
          } community servers.`
        );
        res.json({
          message: `Discovery ${
            resume ? "resumed and" : ""
          } completed successfully`,
          serverCount: servers.length,
        });
      } catch (error) {
        logger.error("Error in manual server discovery:", error);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // 발견 작업 상태 확인 엔드포인트
    app.get("/yunabuju/discover/status", async (req, res) => {
      try {
        const lastBatch = await discovery.getLastUnfinishedBatch();
        if (!lastBatch) {
          return res.json({
            status: "no_pending_batch",
            message: "No unfinished discovery batch found",
          });
        }

        res.json({
          status: "pending",
          batchId: lastBatch.discovery_batch_id,
          totalServers: lastBatch.total_servers,
          pendingServers: lastBatch.pending_servers,
          startedAt: lastBatch.started_at,
        });
      } catch (error) {
        logger.error("Error checking discovery status:", error);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // 서버 상태 체크 엔드포인트
    app.get("/yunabuju/status", async (req, res) => {
      try {
        const allServers = await discovery.getKnownServers(true, false); // 모든 서버 포함
        const communityServers = allServers.filter(
          (server) => !server.is_personal_instance
        );
        const personalServers = allServers.filter(
          (server) => server.is_personal_instance
        );
        const openServers = communityServers.filter(
          (server) => server.registration_open === true
        );

        const stats = {
          status: "healthy",
          lastUpdate: new Date().toISOString(),
          serverCount: {
            total: allServers.length,
            community: communityServers.length,
            personal: personalServers.length,
            openRegistration: openServers.length,
            closedRegistration: communityServers.length - openServers.length,
          },
        };
        res.json(stats);
      } catch (error) {
        logger.error("Error checking status:", error);
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
      console.log(`Server is running on http://localhost:${PORT}`);
    });
  } catch (error) {
    logger.error("Failed to start server:", error);
    throw error;
  }
}

// 서버 시작
startServer().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
