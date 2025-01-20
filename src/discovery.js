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
    this.koreanUsageRate = 0;
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

  async identifyInstanceType(domain, instanceInfo, nodeInfo) {
    let isPersonal = false;
    let instanceType = "unknown";

    try {
      // 1. 사용자 수 기반 체크
      const userCount = instanceInfo.stats?.user_count || 0;
      if (userCount <= 3) {
        isPersonal = true;
      }

      // 2. 설명 기반 체크
      const description = (instanceInfo.description || "").toLowerCase();
      const personalKeywords = [
        "personal",
        "개인",
        "individual",
        "일인",
        "1인",
        "블로그",
      ];
      if (personalKeywords.some((keyword) => description.includes(keyword))) {
        isPersonal = true;
      }

      // 3. 도메인 기반 체크
      const personalDomainKeywords = ["personal", "blog", "개인"];
      if (
        personalDomainKeywords.some((keyword) =>
          domain.toLowerCase().includes(keyword)
        )
      ) {
        isPersonal = true;
      }

      // 4. NodeInfo 기반 체크
      if (nodeInfo?.metadata?.singleUser === true) {
        isPersonal = true;
      }

      // 5. 가입 정책 체크
      if (instanceInfo.registrations?.enabled === false) {
        if (userCount <= 5) {
          isPersonal = true;
        }
      }

      // 인스턴스 유형 결정
      instanceType = isPersonal ? "personal" : "community";
      if (userCount === 0 && !isPersonal) {
        instanceType = "unknown";
      }

      return { isPersonal, instanceType };
    } catch (error) {
      this.logger.error({
        message: "Error identifying instance type",
        domain,
        error: error.message,
      });
      return { isPersonal: null, instanceType: "unknown" };
    }
  }

  async shouldCheckServer(domain) {
    const server = await this.getServerFromDb(domain);
    if (!server) return true;

    // 이미 개인 인스턴스로 확인된 서버는 다시 체크하지 않음
    if (server.is_personal_instance === true) {
      return false;
    }

    if (server.next_check_at && new Date() < new Date(server.next_check_at)) {
      return false;
    }

    return true;
  }

  async processServer(domain) {
    if (!(await this.shouldCheckServer(domain))) {
      const server = await this.getServerFromDb(domain);
      // 개인 인스턴스인 경우 false 반환하여 목록에서 제외
      if (server?.is_personal_instance) {
        return false;
      }
      return server?.is_korean_server || false;
    }

    const startTime = performance.now();

    try {
      const instanceInfo = await this.fetchInstanceInfo(domain);
      if (!instanceInfo) {
        throw new Error("Failed to fetch instance information");
      }

      const nodeInfo = await this.fetchNodeInfo(domain);

      // 인스턴스 유형 먼저 확인
      const { isPersonal, instanceType } = await this.identifyInstanceType(
        domain,
        instanceInfo,
        nodeInfo
      );

      // 개인 인스턴스로 확인되면 더 이상 진행하지 않음
      if (isPersonal) {
        await this.updateServerInDb({
          domain,
          isActive: true,
          isPersonalInstance: true,
          instanceType: "personal",
          ...this.extractServerInfo(instanceInfo),
          hasNodeInfo: !!nodeInfo,
          isKoreanServer: false, // 개인 인스턴스는 한국어 서버 체크 안함
          koreanUsageRate: null,
        });
        return false;
      }

      // 이후 한국어 서버 체크 진행
      const isKorean = await this.checkKoreanSupport(domain, nodeInfo || {});
      if (!isKorean) {
        await this.updateKoreanServerStatus(domain, false);
        return false;
      }

      const koreanUsageRate = await this.analyzeKoreanUsage(domain);
      if (koreanUsageRate <= 0.3) {
        await this.updateKoreanServerStatus(domain, false);
        return false;
      }

      await this.updateServerInDb({
        domain,
        isActive: true,
        koreanUsageRate,
        ...this.extractServerInfo(instanceInfo),
        hasNodeInfo: !!nodeInfo,
        isKoreanServer: true,
        isPersonalInstance: false,
        instanceType: "community",
      });

      await this.updateKoreanServerStatus(domain, true, koreanUsageRate);

      const processingTime = performance.now() - startTime;
      this.logger.info({
        message: "Server processed successfully",
        domain,
        koreanUsageRate,
        processingTime,
        hasNodeInfo: !!nodeInfo,
        instanceType: "community",
      });

      return true;
    } catch (error) {
      await this.updateKoreanServerStatus(domain, false);
      throw new Error(`Error processing ${domain}: ${error.message}`);
    }
  }

  async updateServerInDb(server) {
    const query = `
      INSERT INTO yunabuju_servers 
        (domain, is_active, korean_usage_rate, software_name, software_version,
        registration_open, registration_approval_required, total_users,
        description, has_nodeinfo, is_korean_server, is_personal_instance,
        instance_type, last_checked)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP)
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
        has_nodeinfo = $10,
        is_korean_server = $11,
        is_personal_instance = $12,
        instance_type = $13,
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
      server.hasNodeInfo,
      server.isKoreanServer,
      server.isPersonalInstance,
      server.instanceType,
    ]);
  }

  async getKnownServers(
    includeClosedRegistration = false,
    excludePersonal = false
  ) {
    let query = `
      SELECT * FROM yunabuju_servers 
      WHERE is_active = true AND is_korean_server = true
    `;

    if (!includeClosedRegistration) {
      query += ` AND registration_open = true`;
    }

    if (excludePersonal) {
      query += ` AND (is_personal_instance = false OR is_personal_instance IS NULL)`;
    }

    query += ` ORDER BY korean_usage_rate DESC`;

    const result = await this.pool.query(query);
    return result.rows;
  }

  async startDiscovery() {
    const newServers = await this.discoverNewServers();

    for (const domain of newServers) {
      try {
        // isKoreanInstance가 이미 모든 필요한 검사를 수행합니다
        const isKorean = await this.isKoreanInstance(domain);

        if (isKorean) {
          await this.updateServerInDb({
            domain,
            isActive: true,
            koreanUsageRate: this.koreanUsageRate, // 마지막으로 분석된 비율을 저장할 속성 추가 필요
          });
          this.logger.info(`Added new Korean server: ${domain}`);
        }

        // 서버당 1초 간격
        await new Promise((resolve) => setTimeout(resolve, 1000));
      } catch (error) {
        this.logger.error(`Error processing server ${domain}:`, error);
        continue;
      }
    }
  }

  async shouldCheckKoreanServer(domain) {
    const server = await this.getServerFromDb(domain);
    if (!server) return true;

    // next_korean_check가 설정되어 있고, 아직 체크 시간이 되지 않았으면 캐시된 결과 사용
    if (
      server.next_korean_check &&
      new Date() < new Date(server.next_korean_check)
    ) {
      return false;
    }

    return true;
  }

  async updateKoreanServerStatus(domain, isKorean, koreanUsageRate = null) {
    const query = `
      UPDATE yunabuju_servers 
      SET 
        is_korean_server = $2,
        korean_usage_rate = $3,
        last_korean_check = CURRENT_TIMESTAMP,
        next_korean_check = CASE
          WHEN $2 = false THEN CURRENT_TIMESTAMP + INTERVAL '7 days'  -- 한국어 서버가 아닌 경우 7일 후 재확인
          ELSE NULL  -- 한국어 서버인 경우 매일 체크
        END,
        updated_at = CURRENT_TIMESTAMP
      WHERE domain = $1
    `;

    await this.pool.query(query, [domain, isKorean, koreanUsageRate]);
  }

  async isKoreanInstance(domain) {
    try {
      const nodeInfo = await this.fetchNodeInfo(domain);
      if (!nodeInfo) {
        this.logger.info(`No NodeInfo available for ${domain}`);
        return false;
      }

      const isKoreanServer = await this.checkKoreanSupport(domain, nodeInfo);
      if (!isKoreanServer) {
        this.logger.info(`${domain} is not a Korean server`);
        return false;
      }

      const koreanUsageRate = await this.analyzeKoreanUsage(domain);
      this.koreanUsageRate = koreanUsageRate; // 값을 저장
      const isKorean = koreanUsageRate > 0.3;

      this.logger.info({
        message: `Korean usage analysis for ${domain}`,
        koreanUsageRate,
        isKorean,
      });

      return isKorean;
    } catch (error) {
      this.logger.error({
        message: `Error checking Korean instance ${domain}`,
        error: error.message,
      });
      return false;
    }
  }
}
