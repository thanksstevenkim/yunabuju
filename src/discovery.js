import fetch from "node-fetch";
import { performance } from "perf_hooks";

export class KoreanActivityPubDiscovery {
  constructor(dbPool, logger) {
    this.pool = dbPool;
    this.logger = logger;
    this.seedServers = ["mustard.blog"];
    this.concurrentRequests = process.env.CONCURRENT_REQUESTS || 5;
    this.requestTimeout = process.env.REQUEST_TIMEOUT || 30000;
    this.batchDelay = process.env.BATCH_DELAY || 100;
    this.requestQueue = [];
    this.processing = false;
    this.stats = {
      totalProcessed: 0,
      successful: 0,
      failed: 0,
      startTime: null,
      endTime: null,
    };
  }

  async queueServerCheck(domain) {
    this.requestQueue.push(domain);
    if (!this.processing) {
      await this.processQueue();
    }
  }

  async processQueue() {
    if (this.processing || this.requestQueue.length === 0) return;

    this.processing = true;
    this.stats.startTime = performance.now();

    try {
      while (this.requestQueue.length > 0) {
        const batchStartTime = performance.now();
        const batch = this.requestQueue.splice(0, this.concurrentRequests);

        const results = await Promise.all(
          batch.map((domain) => this.checkServerWithTimeout(domain))
        );

        // 배치 처리 결과 통계
        const successful = results.filter((r) => r === true).length;
        this.stats.successful += successful;
        this.stats.failed += batch.length - successful;
        this.stats.totalProcessed += batch.length;

        const batchTime = performance.now() - batchStartTime;
        this.logger.info({
          message: "Batch processed",
          batchSize: batch.length,
          successful,
          failed: batch.length - successful,
          processingTime: batchTime,
        });

        await new Promise((resolve) => setTimeout(resolve, this.batchDelay));
      }
    } finally {
      this.stats.endTime = performance.now();
      this.processing = false;
      this.logFinalStats();
    }
  }

  logFinalStats() {
    const totalTime = (this.stats.endTime - this.stats.startTime) / 1000;
    this.logger.info({
      message: "Discovery process completed",
      totalProcessed: this.stats.totalProcessed,
      successful: this.stats.successful,
      failed: this.stats.failed,
      totalTimeSeconds: totalTime,
      successRate: `${(
        (this.stats.successful / this.stats.totalProcessed) *
        100
      ).toFixed(2)}%`,
    });
  }

  async checkServerWithTimeout(domain) {
    try {
      const result = await Promise.race([
        this.processServer(domain),
        new Promise((_, reject) =>
          setTimeout(
            () => reject(new Error("Request timeout")),
            this.requestTimeout
          )
        ),
      ]);
      return result;
    } catch (error) {
      this.logger.error({
        message: `Server check failed`,
        domain,
        error: error.message,
        stack: error.stack,
      });
      await this.updateServerFailure(domain);
      return false;
    }
  }

  async processServer(domain) {
    if (!(await this.shouldCheckServer(domain))) {
      return false;
    }

    const startTime = performance.now();

    try {
      const [instanceInfo, nodeInfo] = await Promise.all([
        this.fetchInstanceInfo(domain),
        this.fetchNodeInfo(domain),
      ]);

      if (!instanceInfo || !nodeInfo) {
        throw new Error("Failed to fetch server information");
      }

      const isKorean = await this.checkKoreanSupport(domain, nodeInfo);
      if (!isKorean) {
        return false;
      }

      const koreanUsageRate = await this.analyzeKoreanUsage(domain);
      if (koreanUsageRate <= 0.3) {
        return false;
      }

      await this.updateServerInDb({
        domain,
        isActive: true,
        koreanUsageRate,
        ...this.extractServerInfo(instanceInfo),
      });

      const processingTime = performance.now() - startTime;
      this.logger.info({
        message: "Server processed successfully",
        domain,
        koreanUsageRate,
        processingTime,
      });

      return true;
    } catch (error) {
      throw new Error(`Error processing ${domain}: ${error.message}`);
    }
  }

  async fetchWithBackoff(url, options = {}, attempts = 3) {
    for (let i = 0; i < attempts; i++) {
      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10000);

        const response = await fetch(url, {
          ...options,
          signal: controller.signal,
          headers: {
            "User-Agent": "Yunabuju-ActivityPub-Directory/1.0",
            ...options.headers,
          },
        });

        clearTimeout(timeout);

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        return response;
      } catch (error) {
        if (i === attempts - 1) throw error;
        await new Promise((resolve) =>
          setTimeout(resolve, 1000 * Math.pow(2, i))
        );
      }
    }
  }

  async shouldCheckServer(domain) {
    const server = await this.getServerFromDb(domain);
    if (!server) return true;

    if (server.next_check_at && new Date() < new Date(server.next_check_at)) {
      return false;
    }

    return true;
  }

  async updateServerFailure(domain) {
    const query = `
      UPDATE yunabuju_servers 
      SET 
        failed_attempts = COALESCE(failed_attempts, 0) + 1,
        last_failed_at = CURRENT_TIMESTAMP,
        next_check_at = CURRENT_TIMESTAMP + (INTERVAL '1 hour' * POWER(2, LEAST(COALESCE(failed_attempts, 0), 7))),
        is_active = false,
        updated_at = CURRENT_TIMESTAMP
      WHERE domain = $1
      RETURNING *;
    `;

    try {
      await this.pool.query(query, [domain]);
    } catch (error) {
      this.logger.error({
        message: "Failed to update server failure status",
        domain,
        error: error.message,
      });
    }
  }

  async resetServerFailure(domain) {
    const query = `
      UPDATE yunabuju_servers 
      SET 
        failed_attempts = 0,
        last_failed_at = NULL,
        next_check_at = NULL,
        is_active = true,
        updated_at = CURRENT_TIMESTAMP
      WHERE domain = $1;
    `;

    try {
      await this.pool.query(query, [domain]);
    } catch (error) {
      this.logger.error({
        message: "Failed to reset server failure status",
        domain,
        error: error.message,
      });
    }
  }

  containsKorean(text) {
    if (!text) return false;
    const koreanRegex =
      /[\uAC00-\uD7AF\u1100-\u11FF\u3130-\u318F\uA960-\uA97F\uD7B0-\uD7FF]/;
    return koreanRegex.test(text);
  }

  async checkKoreanSupport(domain, nodeInfo) {
    try {
      if (nodeInfo.metadata?.languages?.includes("ko")) {
        return true;
      }

      const instanceInfo = await this.fetchInstanceInfo(domain);
      if (instanceInfo.languages?.includes("ko")) {
        return true;
      }

      return (
        this.containsKorean(instanceInfo.description) ||
        instanceInfo.rules?.some((rule) => this.containsKorean(rule.text))
      );
    } catch (error) {
      return false;
    }
  }

  extractServerInfo(instanceInfo) {
    return {
      softwareName: instanceInfo.software?.name,
      softwareVersion: instanceInfo.software?.version,
      registrationOpen: instanceInfo.registrations?.enabled,
      registrationApprovalRequired: instanceInfo.approval_required,
      totalUsers: instanceInfo.stats?.user_count,
      description: instanceInfo.description,
    };
  }

  async fetchNodeInfo(domain) {
    try {
      const response = await this.fetchWithBackoff(
        `https://${domain}/.well-known/nodeinfo`
      );
      const links = await response.json();

      const nodeInfoLink = links.links.find(
        (link) => link.rel === "http://nodeinfo.diaspora.software/ns/schema/2.0"
      );

      if (!nodeInfoLink) return null;

      const nodeInfoResponse = await this.fetchWithBackoff(nodeInfoLink.href);
      return await nodeInfoResponse.json();
    } catch (error) {
      return null;
    }
  }

  async fetchInstanceInfo(domain) {
    try {
      const response = await this.fetchWithBackoff(
        `https://${domain}/api/v1/instance`
      );
      return await response.json();
    } catch (error) {
      return null;
    }
  }

  async analyzeKoreanUsage(domain) {
    try {
      const response = await this.fetchWithBackoff(
        `https://${domain}/api/v1/timelines/public?local=true&limit=20`
      );
      const posts = await response.json();

      if (!posts.length) return 0;

      const koreanPosts = posts.filter((post) =>
        this.containsKorean(post.content)
      );
      return koreanPosts.length / posts.length;
    } catch (error) {
      return 0;
    }
  }

  async discoverNewServers() {
    const newServers = new Set();

    for (const server of this.seedServers) {
      try {
        const peers = await this.fetchPeers(server);
        for (const peer of peers) {
          if (!(await this.getServerFromDb(peer))) {
            newServers.add(peer);
          }
        }
      } catch (error) {
        this.logger.error({
          message: "Error discovering peers",
          server,
          error: error.message,
        });
      }
    }

    return Array.from(newServers);
  }

  async fetchPeers(server) {
    const endpoints = [
      `https://${server}/api/v1/instance/peers`,
      `https://${server}/api/v1/federation/peers`,
    ];

    for (const endpoint of endpoints) {
      try {
        const response = await this.fetchWithBackoff(endpoint);
        const peers = await response.json();
        return Array.isArray(peers) ? peers : Object.keys(peers);
      } catch (error) {
        continue;
      }
    }

    return [];
  }

  async getServerFromDb(domain) {
    const result = await this.pool.query(
      "SELECT * FROM yunabuju_servers WHERE domain = $1",
      [domain]
    );
    return result.rows[0];
  }

  async updateServerInDb(server) {
    const query = `
      INSERT INTO yunabuju_servers 
        (domain, is_active, korean_usage_rate, software_name, software_version,
         registration_open, registration_approval_required, total_users,
         description, last_checked)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP)
      ON CONFLICT (domain) 
      DO UPDATE SET 
        is_active = $2,
        korean_usage_rate = $3,
        software_name = $4,
        software_version = $5,
        registration_open = $6,
        registration_approval_required = $7,
        total_users = $8,
        description = $9,
        last_checked = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    `;

    await this.pool.query(query, [
      server.domain,
      server.isActive,
      server.koreanUsageRate,
      server.softwareName,
      server.softwareVersion,
      server.registrationOpen,
      server.registrationApprovalRequired,
      server.totalUsers,
      server.description,
    ]);
  }

  async getKnownServers(includeClosedRegistration = false) {
    const query = includeClosedRegistration
      ? "SELECT * FROM yunabuju_servers WHERE is_active = true ORDER BY korean_usage_rate DESC"
      : "SELECT * FROM yunabuju_servers WHERE is_active = true AND registration_open = true ORDER BY korean_usage_rate DESC";
    const result = await this.pool.query(query);
    return result.rows;
  }

  async startDiscovery() {
    const newServers = await this.discoverNewServers();

    for (const domain of newServers) {
      try {
        const isKorean = await this.isKoreanInstance(domain);
        if (isKorean) {
          const koreanUsageRate = await this.analyzeKoreanUsage(domain);
          await this.updateServerInDb({
            domain,
            isActive: true,
            koreanUsageRate,
          });
          this.logger.info(`Added new Korean server: ${domain}`);
        }

        // 서버당 1초 간격
        await new Promise((resolve) => setTimeout(resolve, 1000));
      } catch (error) {
        this.logger.error(`Error processing server ${domain}:`, error);
      }
    }
  }
}
