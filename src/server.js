import express from "express";
import cors from "cors";
import pg from "pg";
import { KoreanActivityPubDiscovery } from "./discovery.js";
import cron from "node-cron";
import winston from "winston";
import "winston-daily-rotate-file"; // 추가
import dotenv from "dotenv";
import { setupDatabase } from "./database.js";
import suspiciousDomainsData from "../suspicious-domains.json" assert { type: "json" };
import fs from "fs";
import path from "path";
import session from "express-session";
import punycode from "punycode";

dotenv.config(); // Ensure .env is loaded at the top

// logs 디렉토리 생성
const logsDir = path.join(process.cwd(), "logs");
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir);
}

const { suspiciousDomains } = suspiciousDomainsData;

// 로그 설정 수정
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    // 에러 로그 설정
    new winston.transports.DailyRotateFile({
      filename: "logs/error-%DATE%.log",
      datePattern: "YYYY-MM-DD",
      level: "error",
      maxSize: "20m", // 20MB 초과시 새 파일
      maxFiles: "14d", // 14일치 보관
      zippedArchive: true, // 오래된 로그 압축
    }),
    // 전체 로그 설정
    new winston.transports.DailyRotateFile({
      filename: "logs/combined-%DATE%.log",
      datePattern: "YYYY-MM-DD",
      maxSize: "20m",
      maxFiles: "14d",
      zippedArchive: true,
    }),
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

    // 세션 미들웨어 추가
    app.use(
      session({
        secret: process.env.SESSION_SECRET || "yunabuju-secret",
        resave: false,
        saveUninitialized: false,
        cookie: {
          secure: process.env.NODE_ENV === "production",
          httpOnly: true,
          maxAge: 24 * 60 * 60 * 1000, // 24시간
        },
      })
    );

    app.use(cors());
    app.use(express.json());

    // Middleware to block suspicious domains
    app.use((req, res, next) => {
      const domain = req.hostname;
      if (suspiciousDomains.includes(domain)) {
        logger.warn(`Blocked access to suspicious domain: ${domain}`);
        return res
          .status(403)
          .json({ error: "Access to this domain is blocked" });
      }
      next();
    });

    // Middleware to check admin access
    function checkAdmin(req, res, next) {
      const submittedPassword = req.query.password || req.body.password;
      const adminPassword = process.env.ADMIN_PASSWORD;

      if (!adminPassword) {
        logger.error("ADMIN_PASSWORD not set in environment variables");
        return res.status(500).json({ error: "Server configuration error" });
      }

      if (submittedPassword === adminPassword) {
        next();
      } else {
        res.status(403).json({ error: "Invalid admin password" });
      }
    }

    // 새로운 인증 및 세션 미들웨어 설정
    app.use(
      session({
        secret: process.env.SESSION_SECRET || "yunabuju-secret",
        resave: false,
        saveUninitialized: false,
        cookie: { secure: process.env.NODE_ENV === "production" },
      })
    );

    // 인증 미들웨어
    const authMiddleware = (req, res, next) => {
      if (req.session?.isAdmin) {
        next();
      } else {
        res.redirect("/yunabuju/admin");
      }
    };

    // 로그인 처리
    app.post(
      "/yunabuju/auth",
      express.urlencoded({ extended: true }),
      (req, res) => {
        const { password } = req.body;
        if (password === process.env.ADMIN_PASSWORD) {
          req.session.isAdmin = true;
          res.redirect("/yunabuju/servers/all");
        } else {
          res.redirect("/yunabuju/admin");
        }
      }
    );

    // 디스커버리 인스턴스 생성
    const discovery = new KoreanActivityPubDiscovery(pool, logger);

    // 시작할 때 기존 데이터 정리
    await discovery.cleanupLowUsageServers();

    // resume 명령어 체크
    if (process.argv.includes("resume")) {
      await discovery.resumeDiscovery();
    }

    // 루트 경로에 상태 및 기본 정보 제공
    app.get("/", (req, res) => {
      res.json({
        name: "Yunabuju - Korean ActivityPub Directory",
        status: "running",
        endpoints: {
          servers: "/yunabuju/servers",
          discoverStatus: "/yunabuju/discover/status",
          status: "/yunabuju/status",
        },
        version: "1.0.0",
      });
    });

    // HTML 테이블 생성 함수 추가
    function createServerTableHtml(servers, isAdminView = false) {
      const decodeDomain = (domain) => {
        try {
          return punycode.toUnicode(domain);
        } catch (error) {
          return domain;
        }
      };

      const createTableRow = (server) => {
        const decodedDomain = decodeDomain(server.domain);
        const domainCell = `<td><a href="https://${server.domain}" target="_blank" rel="noopener noreferrer">${decodedDomain}</a></td>`;

        if (isAdminView) {
          return `
            <tr>
              <td>${server.node_name || "Unknown"}</td>
              ${domainCell}
              <td>${server.node_description || server.description || ""}</td>
              <td>${server.instance_type || "unknown"}</td>
              <td>${server.is_active ? "Active" : "Inactive"}</td>
              <td>${server.total_users || "N/A"}</td>
              <td>${
                server.korean_usage_rate
                  ? (server.korean_usage_rate * 100).toFixed(1) + "%"
                  : "N/A"
              }</td>
              <td>${server.software_name || "N/A"} ${
            server.software_version || ""
          }</td>
              <td>${new Date(server.last_checked).toLocaleString()}</td>
            </tr>
          `;
        } else {
          return `
            <tr>
              <td>${server.node_name || "Unknown"}</td>
              ${domainCell}
              <td>${server.node_description || server.description || ""}</td>
              <td>${server.software_name || "N/A"} ${
            server.software_version || ""
          }</td>
            </tr>
          `;
        }
      };

      return servers
        .sort((a, b) => (b.total_users || 0) - (a.total_users || 0))
        .map(createTableRow)
        .join("");
    }

    // 기존 /yunabuju/servers 엔드포인트 수정
    app.get("/yunabuju/servers", async (req, res) => {
      try {
        const servers = await discovery.getKnownServers(false, true);

        const html = `
          <html>
            <head>
              <title>Yunabuju Server List</title>
              <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                table { width: 100%; border-collapse: collapse; }
                th, td { padding: 8px; text-align: left; border: 1px solid #ddd; }
                th { background-color: #f4f4f4; }
                tr:nth-child(even) { background-color: #f9f9f9; }
                a { color: #007bff; text-decoration: none; }
                a:hover { text-decoration: underline; }
              </style>
            </head>
            <body>
              <h1>Korean ActivityPub Server List</h1>
              <table>
                <thead>
                  <tr>
                    <th>Server Name</th>
                    <th>Domain</th>
                    <th>Description</th>
                    <th>Software</th>
                  </tr>
                </thead>
                <tbody>
                  ${createServerTableHtml(servers)}
                </tbody>
              </table>
            </body>
          </html>
        `;

        res.send(html);
      } catch (error) {
        logger.error("Error fetching servers:", error);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // 기존 /yunabuju/servers/all 엔드포인트 수정
    app.get("/yunabuju/servers/all", authMiddleware, async (req, res) => {
      try {
        const includePersonal = req.query.includePersonal === "true";

        // 세션에 현재 상태 저장
        req.session.showPersonal = includePersonal;

        const servers = await discovery.getKnownServers(true, !includePersonal);

        const html = `
          <html>
            <head>
              <title>Yunabuju Server List</title>
              <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                table { width: 100%; border-collapse: collapse; }
                th, td { padding: 8px; text-align: left; border: 1px solid #ddd; }
                th { background-color: #f4f4f4; }
                tr:nth-child(even) { background-color: #f9f9f9; }
                .filters { margin-bottom: 20px; }
                .filters label { margin-right: 15px; }
                a { color: #007bff; text-decoration: none; }
                a:hover { text-decoration: underline; }
              </style>
              <script>
                function updateFilter(checkbox) {
                  window.location.href = '/yunabuju/servers/all?includePersonal=' + checkbox.checked;
                }
              </script>
            </head>
            <body>
              <h1>Yunabuju Server List (Admin View)</h1>
              <div class="filters">
                <label>
                  <input 
                    type="checkbox" 
                    ${includePersonal ? "checked" : ""}
                    onchange="updateFilter(this)"
                  >
                  Include Personal Instances
                </label>
              </div>
              <table>
                <thead>
                  <tr>
                    <th>Server Name</th>
                    <th>Domain</th>
                    <th>Description</th>
                    <th>Type</th>
                    <th>Status</th>
                    <th>Users</th>
                    <th>Korean Usage</th>
                    <th>Software</th>
                    <th>Last Checked</th>
                  </tr>
                </thead>
                <tbody>
                  ${createServerTableHtml(servers, true)}
                </tbody>
              </table>
            </body>
          </html>
        `;

        res.send(html);
      } catch (error) {
        logger.error("Error fetching all servers:", error);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // API 라우트 수정 - 기본 서버 목록 (가입 가능한 커뮤니티 서버만)
    app.get("/yunabuju/servers", async (req, res) => {
      try {
        const servers = await discovery.getKnownServers(false, true);

        const html = `
          <html>
            <head>
              <title>Yunabuju Server List</title>
              <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                table { width: 100%; border-collapse: collapse; }
                th, td { padding: 8px; text-align: left; border: 1px solid #ddd; }
                th { background-color: #f4f4f4; }
                tr:nth-child(even) { background-color: #f9f9f9; }
              </style>
            </head>
            <body>
              <h1>Korean ActivityPub Server List</h1>
              <table>
                <thead>
                  <tr>
                    <th>Server Name</th>
                    <th>Domain</th>
                    <th>Description</th>
                    <th>Software</th>
                  </tr>
                </thead>
                <tbody>
                  ${servers
                    .sort((a, b) => (b.total_users || 0) - (a.total_users || 0))
                    .map(
                      (server) => `
                    <tr>
                      <td>${server.node_name || "Unknown"}</td>
                      <td>${server.domain}</td>
                      <td>${
                        server.node_description || server.description || ""
                      }</td>
                      <td>${server.software_name || "N/A"} ${
                        server.software_version || ""
                      }</td>
                    </tr>
                  `
                    )
                    .join("")}
                </tbody>
              </table>
            </body>
          </html>
        `;

        res.send(html);
      } catch (error) {
        logger.error("Error fetching servers:", error);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // Admin login page - 수정된 버전
    app.get("/yunabuju/admin", (req, res) => {
      res.send(`
        <html>
          <head>
            <title>Yunabuju Admin Login</title>
            <style>
              body { font-family: Arial, sans-serif; margin: 40px; }
              .container { max-width: 400px; margin: 0 auto; }
              .form-group { margin-bottom: 15px; }
              label { display: block; margin-bottom: 5px; }
              input { width: 100%; padding: 8px; }
              button { padding: 10px 15px; background: #007bff; color: white; border: none; cursor: pointer; }
              button:hover { background: #0056b3; }
            </style>
          </head>
          <body>
            <div class="container">
              <h1>Yunabuju Admin Access</h1>
              <form action="/yunabuju/auth" method="POST">
                <div class="form-group">
                  <label for="password">Admin Password:</label>
                  <input type="password" id="password" name="password" required>
                </div>
                <button type="submit">Access Admin Data</button>
              </form>
            </div>
          </body>
        </html>
      `);
    });

    // Protected admin data endpoint - 수정된 버전
    app.get("/yunabuju/servers/all", authMiddleware, async (req, res) => {
      try {
        const includePersonal = req.session.showPersonal === true; // 명시적 비교
        const servers = await discovery.getKnownServers(true, !includePersonal);

        const html = `
          <html>
            <head>
              <title>Yunabuju Server List</title>
              <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                table { width: 100%; border-collapse: collapse; }
                th, td { padding: 8px; text-align: left; border: 1px solid #ddd; }
                th { background-color: #f4f4f4; }
                tr:nth-child(even) { background-color: #f9f9f9; }
                .filters { margin-bottom: 20px; }
                .filters label { margin-right: 15px; }
              </style>
            </head>
            <body>
              <h1>Yunabuju Server List (Admin View)</h1>
              <div class="filters">
                <form method="POST" action="/yunabuju/servers/all/filters">
                  <label>
                    <input 
                      type="checkbox" 
                      name="includePersonal"
                      ${includePersonal ? "checked" : ""}
                      onchange="this.form.submit()"
                    >
                    Include Personal Instances
                  </label>
                </form>
              </div>
              <table>
                <thead>
                  <tr>
                    <th>Server Name</th>
                    <th>Domain</th>
                    <th>Description</th>
                    <th>Type</th>
                    <th>Status</th>
                    <th>Users</th>
                    <th>Korean Usage</th>
                    <th>Software</th>
                    <th>Last Checked</th>
                  </tr>
                </thead>
                <tbody>
                  ${createServerTableHtml(servers, true)}
                </tbody>
              </table>
            </body>
          </html>
        `;

        res.send(html);
      } catch (error) {
        logger.error("Error fetching all servers:", error);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // 필터 설정 처리 - 수정된 버전
    app.post("/yunabuju/servers/all/filters", authMiddleware, (req, res) => {
      const includePersonal = req.body.includePersonal === "on";
      req.session.showPersonal = includePersonal; // 세션에 상태 저장
      req.session.save(() => {
        res.redirect("/yunabuju/servers/all");
      });
    });

    // 수동으로 서버 검색을 시작하는 엔드포인트
    app.post("/yunabuju/discover", async (req, res) => {
      try {
        const { resume, preserveExisting, resetBatch } = req.query;
        logger.info(
          `${
            resume ? "Resuming" : "Starting"
          } manual server discovery... (preserveExisting: ${
            preserveExisting === "true"
          }, resetBatch: ${resetBatch === "true"})`
        );

        if (resume === "true") {
          const result = await discovery.resumeDiscovery();
          if (!result) {
            return res.json({ message: "No unfinished batch found to resume" });
          }
        } else {
          // preserveExisting이 true인 경우 새로운 batchId 생성
          const batchId =
            preserveExisting === "true" ? `existing-${Date.now()}` : null;
          await discovery.startDiscovery(batchId, resetBatch === "true");
        }

        const servers = await discovery.getKnownServers(true, true);
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
    // server.js의 status 엔드포인트 수정
    app.get("/yunabuju/discover/status", async (req, res) => {
      try {
        // 현재 진행 중인 배치 정보 조회
        const query = `
      SELECT 
        discovery_batch_id,
        COUNT(*) as total_servers,
        COUNT(*) FILTER (WHERE discovery_status = 'pending') as pending_count,
        COUNT(*) FILTER (WHERE discovery_status = 'in_progress') as in_progress_count,
        COUNT(*) FILTER (WHERE discovery_status = 'completed') as completed_count,
        COUNT(*) FILTER (WHERE discovery_status = 'failed') as failed_count,
        COUNT(*) FILTER (WHERE discovery_status = 'not_korean') as not_korean_count,
        MIN(discovery_started_at) as started_at,
        MAX(discovery_completed_at) as last_completion,
        STRING_AGG(DISTINCT discovery_status, ', ') as current_statuses
      FROM yunabuju_servers
      WHERE discovery_started_at > NOW() - INTERVAL '1 day'
      GROUP BY discovery_batch_id
      ORDER BY started_at DESC
      LIMIT 1
    `;

        const result = await pool.query(query);

        if (result.rows.length === 0) {
          return res.json({
            status: "no_active_batch",
            message: "No active discovery batch found in the last 24 hours",
          });
        }

        const batchInfo = result.rows[0];
        const elapsedTime = new Date() - new Date(batchInfo.started_at);
        const elapsedMinutes = Math.floor(elapsedTime / 60000);

        res.json({
          status: "active",
          batchId: batchInfo.discovery_batch_id,
          startedAt: batchInfo.started_at,
          elapsedMinutes,
          progress: {
            total: parseInt(batchInfo.total_servers),
            pending: parseInt(batchInfo.pending_count),
            inProgress: parseInt(batchInfo.in_progress_count),
            completed: parseInt(batchInfo.completed_count),
            failed: parseInt(batchInfo.failed_count),
            notKorean: parseInt(batchInfo.not_korean_count),
          },
          completionPercentage: Math.floor(
            ((parseInt(batchInfo.completed_count) +
              parseInt(batchInfo.failed_count) +
              parseInt(batchInfo.not_korean_count)) /
              parseInt(batchInfo.total_servers)) *
              100
          ),
          currentStatuses: batchInfo.current_statuses,
          lastCompletion: batchInfo.last_completion,
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
