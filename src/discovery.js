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

  async processPendingServers(batchId) {
    const query = `
      SELECT domain 
      FROM yunabuju_servers 
      WHERE discovery_batch_id = $1 
        AND discovery_status = 'pending'
      ORDER BY domain
    `;

    try {
      const { rows } = await this.pool.query(query, [batchId]);
      this.logger.info({
        message: "Starting to process pending servers",
        batchId,
        serverCount: rows.length,
      });

      for (const row of rows) {
        try {
          // 상태를 in_progress로 변경
          await this.updateServerStatus(row.domain, batchId, "in_progress");

          const isKorean = await this.isKoreanInstance(row.domain);
          if (isKorean) {
            await this.updateServerInDb({
              domain: row.domain,
              isActive: true,
              koreanUsageRate: this.koreanUsageRate,
            });
            await this.updateServerStatus(row.domain, batchId, "completed");
            this.logger.info(`Added new Korean server: ${row.domain}`);
          } else {
            await this.updateServerStatus(row.domain, batchId, "not_korean");
          }

          // 서버당 1초 간격 유지
          await new Promise((resolve) => setTimeout(resolve, 1000));
        } catch (error) {
          await this.updateServerStatus(row.domain, batchId, "failed");
          this.logger.error(`Error processing server ${row.domain}:`, error);
          continue;
        }
      }
    } catch (error) {
      this.logger.error({
        message: "Error processing pending servers",
        batchId,
        error: error.message,
      });
      throw error;
    }
  }

  async updateServerStatus(domain, batchId, status) {
    const query = `
      UPDATE yunabuju_servers 
      SET 
        discovery_status = $3::varchar,
        discovery_completed_at = CASE 
          WHEN $3::varchar IN ('completed', 'failed', 'not_korean') THEN CURRENT_TIMESTAMP 
          ELSE discovery_completed_at 
        END
      WHERE domain = $1 AND discovery_batch_id = $2
    `;

    try {
      await this.pool.query(query, [domain, batchId, String(status)]);
      this.logger.debug({
        message: "Updated server status",
        domain,
        batchId,
        status,
      });
    } catch (error) {
      this.logger.error({
        message: "Error updating server status",
        domain,
        batchId,
        status,
        error: error.message,
      });
      throw error;
    }
  }

  async updateServerDiscoveryStatus(domain, batchId, status) {
    const query = `
      UPDATE yunabuju_servers 
      SET 
        discovery_status = $3,
        discovery_completed_at = CURRENT_TIMESTAMP
      WHERE domain = $1 AND discovery_batch_id = $2
    `;

    await this.pool.query(query, [domain, batchId, String(status)]); // status를 문자열로 변환
  }

  async identifyInstanceType(domain, instanceInfo, nodeInfo) {
    let isPersonal = false;
    let instanceType = "unknown";
    let matchedDescription = null;

    try {
      // 1. 각각의 설명을 개별적으로 확인
      const descriptions = [
        { text: instanceInfo?.description, source: "description" },
        { text: instanceInfo?.short_description, source: "short_description" },
        { text: instanceInfo?.branding_server_description, source: "branding" },
        { text: instanceInfo?.extended_description, source: "extended" },
      ];

      // 개인 서버 키워드
      const personalKeywords = ["개인적", "개인", "개인용", "1인", "비공개"];

      // 각 설명을 개별적으로 확인
      for (const desc of descriptions) {
        if (!desc.text) continue;

        const descText = desc.text.toLowerCase();
        for (const keyword of personalKeywords) {
          if (descText.includes(keyword)) {
            isPersonal = true;
            matchedDescription = {
              source: desc.source,
              keyword: keyword,
              text: desc.text,
            };
            break;
          }
        }
        if (isPersonal) break; // 하나라도 찾으면 중단
      }

      // 2. 도메인 기반 체크
      if (!isPersonal) {
        const personalDomainKeywords = ["personal", "blog", "개인"];
        if (
          personalDomainKeywords.some((keyword) =>
            domain.toLowerCase().includes(keyword)
          )
        ) {
          isPersonal = true;
          matchedDescription = {
            source: "domain",
            keyword: "domain-based",
            text: domain,
          };
        }
      }

      // 3. NodeInfo 기반 체크
      if (!isPersonal && nodeInfo?.metadata?.singleUser === true) {
        isPersonal = true;
        matchedDescription = {
          source: "nodeinfo",
          keyword: "singleUser",
          text: "nodeinfo.metadata.singleUser === true",
        };
      }

      // 4. 사용자 수 기반 체크 (보조 지표로만 사용)
      const userCount = instanceInfo?.stats?.user_count || 0;
      if (!isPersonal && userCount <= 3) {
        isPersonal = true;
        matchedDescription = {
          source: "user_count",
          keyword: "low_users",
          text: `user count: ${userCount}`,
        };
      }

      // 5. 가입 정책 체크 (보조 지표)
      if (
        !isPersonal &&
        instanceInfo?.registrations?.enabled === false &&
        userCount <= 5
      ) {
        isPersonal = true;
        matchedDescription = {
          source: "registration",
          keyword: "closed_registration",
          text: "closed registration with low user count",
        };
      }

      // 인스턴스 유형 결정
      instanceType = isPersonal ? "personal" : "community";
      if (userCount === 0 && !isPersonal) {
        instanceType = "unknown";
      }

      // 로깅
      if (isPersonal) {
        this.logger.debug({
          message: "Personal instance detected",
          domain,
          matchedDescription,
        });
      }

      return { isPersonal, instanceType, matchedDescription };
    } catch (error) {
      this.logger.error({
        message: "Error identifying instance type",
        domain,
        error: error.message,
      });
      return {
        isPersonal: null,
        instanceType: "unknown",
        matchedDescription: null,
      };
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
      extendedDescription: instanceInfo.extended_description,
      brandingServerDescription: instanceInfo.branding_server_description,
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
    // NodeInfo를 기본 정보 소스로 사용
    try {
      const nodeInfo = await this.fetchNodeInfo(domain);
      if (nodeInfo) {
        // 기본 정보 구성
        const instanceInfo = this.convertNodeInfoToInstanceInfo(nodeInfo);

        // 서버별 추가 정보 가져오기 시도
        try {
          const softwareName = nodeInfo.software?.name?.toLowerCase();
          let extraInfo = {};

          if (softwareName === "misskey") {
            // Misskey 추가 정보
            const response = await this.fetchWithBackoff(
              `https://${domain}/api/meta`,
              {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({}),
              }
            );
            const misskeyData = await response.json();
            extraInfo = {
              description: misskeyData.description || instanceInfo.description,
              registrations: {
                enabled: misskeyData.disableRegistration !== true,
              },
              rules: Array.isArray(misskeyData.policies)
                ? misskeyData.policies.map((p) => ({ text: p }))
                : [],
            };
          } else if (
            softwareName === "mastodon" ||
            softwareName === "pleroma"
          ) {
            // Mastodon과 Pleroma의 엔드포인트 시도
            const endpoints =
              softwareName === "pleroma"
                ? ["/api/instance", "/api/v1/instance", "/api/v2/instance"] // Pleroma는 /api/instance를 우선
                : ["/api/v1/instance", "/api/v2/instance", "/api/instance"]; // Mastodon은 v1을 우선

            let data = null;
            for (const endpoint of endpoints) {
              try {
                const response = await this.fetchWithBackoff(
                  `https://${domain}${endpoint}`
                );
                data = await response.json();
                break; // 성공하면 반복 중단
              } catch (error) {
                this.logger.debug(
                  `Failed to fetch ${endpoint} for ${domain}: ${error.message}`
                );
                continue;
              }
            }

            if (!data) {
              throw new Error("All instance API endpoints failed");
            }

            extraInfo = {
              description: data.description || instanceInfo.description,
              rules: data.rules || [],
              registrations: {
                enabled: data.registrations?.enabled ?? !data.closed,
              },
            };
          }

          // 기본 정보와 추가 정보 병합
          return {
            ...instanceInfo,
            ...extraInfo,
          };
        } catch (error) {
          // 추가 정보 가져오기 실패시 기본 NodeInfo 데이터만 사용
          this.logger.debug(
            `Failed to fetch additional info for ${domain}: ${error.message}`
          );
          return instanceInfo;
        }
      }
    } catch (error) {
      this.logger.debug(
        `NodeInfo fetch failed for ${domain}: ${error.message}`
      );
    }

    throw new Error(`Failed to fetch instance info from any endpoint`);
  }

  convertNodeInfoToInstanceInfo(nodeInfo) {
    return {
      software: {
        name: nodeInfo.software?.name || "unknown",
        version: nodeInfo.software?.version,
      },
      stats: {
        user_count: nodeInfo.usage?.users?.total || 0,
      },
      languages: nodeInfo.metadata?.languages || [],
      registrations: {
        enabled: nodeInfo.openRegistrations !== false,
      },
      description: nodeInfo.metadata?.nodeName || "",
      rules: [],
    };
  }

  async analyzeKoreanUsage(domain) {
    try {
      const nodeInfo = await this.fetchNodeInfo(domain);
      const softwareName = nodeInfo?.software?.name?.toLowerCase();
      const endpoint =
        softwareName === "misskey"
          ? {
              url: `https://${domain}/api/notes/local-timeline`,
              method: "POST",
              body: { limit: 50, withFiles: false },
            }
          : {
              url: `https://${domain}/api/v1/timelines/public?local=true&limit=50`,
              method: "GET",
            };

      const response = await this.fetchWithBackoff(endpoint.url, {
        method: endpoint.method,
        headers: { "Content-Type": "application/json" },
        ...(endpoint.body ? { body: JSON.stringify(endpoint.body) } : {}),
      });

      const posts = await response.json();
      if (!Array.isArray(posts) || posts.length === 0) {
        return await this.analyzeServerMetadata(domain);
      }

      const validPosts = posts.filter((post) => {
        const content = softwareName === "misskey" ? post.text : post.content;
        return typeof content === "string" && content.length > 0;
      });

      if (validPosts.length === 0) {
        return await this.analyzeServerMetadata(domain);
      }

      const koreanPosts = validPosts.filter((post) => {
        const content =
          softwareName === "misskey" ? post.text : this.stripHtml(post.content);
        const cleanContent = content.replace(
          /[\uD800-\uDBFF][\uDC00-\uDFFF]/g,
          ""
        );
        return this.containsKorean(cleanContent);
      });

      return koreanPosts.length / validPosts.length;
    } catch (error) {
      this.logger.warn(`Error in analyzeKoreanUsage for ${domain}:`, error);
      return await this.analyzeServerMetadata(domain);
    }
  }

  stripHtml(html) {
    return html.replace(/<[^>]*>/g, "");
  }

  async analyzeServerMetadata(domain) {
    try {
      // 인스턴스 정보에서 한국어 사용 여부 확인
      const instanceInfo = await this.fetchInstanceInfo(domain);
      if (!instanceInfo) return 0;

      let koreanPoints = 0;
      let totalPoints = 0;

      // 서버 설명에서 한국어 확인
      if (instanceInfo.description) {
        totalPoints++;
        if (this.containsKorean(instanceInfo.description)) koreanPoints++;
      }

      // 서버 규칙에서 한국어 확인
      if (Array.isArray(instanceInfo.rules)) {
        totalPoints += instanceInfo.rules.length;
        koreanPoints += instanceInfo.rules.filter((rule) =>
          this.containsKorean(rule.text)
        ).length;
      }

      // 언어 설정 확인
      if (Array.isArray(instanceInfo.languages)) {
        totalPoints++;
        if (instanceInfo.languages.includes("ko")) koreanPoints++;
      }

      return totalPoints > 0 ? koreanPoints / totalPoints : 0;
    } catch (error) {
      this.logger.error(`Error in analyzeServerMetadata for ${domain}:`, error);
      return 0;
    }
  }

  extractNextLink(linkHeader) {
    if (!linkHeader) return null;
    const links = linkHeader.split(",");
    const nextLink = links.find((link) => link.includes('rel="next"'));
    if (!nextLink) return null;

    const matches = nextLink.match(/<([^>]+)>/);
    return matches ? matches[1] : null;
  }

  async discoverNewServers(depth = 3) {
    const newServers = new Set();
    const processedServers = new Set();
    let currentDepthServers = new Set(this.seedServers);

    this.logger.info({
      message: "Starting server discovery",
      seedServers: Array.from(this.seedServers),
      maxDepth: depth,
    });

    for (let currentDepth = 0; currentDepth < depth; currentDepth++) {
      const currentServers = Array.from(currentDepthServers);
      this.logger.info({
        message: `Starting depth ${currentDepth + 1} discovery`,
        depth: currentDepth + 1,
        serversToProcess: currentServers.length,
        processedSoFar: processedServers.size,
        newServersFound: newServers.size,
        currentServers,
      });

      const nextDepthServers = new Set();
      const unprocessedServers = currentServers.filter(
        (server) => !processedServers.has(server)
      );

      if (unprocessedServers.length > 0) {
        try {
          const peers = await this.fetchPeers(unprocessedServers);

          for (const peer of peers) {
            if (!processedServers.has(peer)) {
              if (!(await this.getServerFromDb(peer))) {
                newServers.add(peer);
                this.logger.debug({
                  message: "Found new server",
                  peer,
                  depth: currentDepth + 1,
                });
              }
              nextDepthServers.add(peer);
            }
          }
        } catch (error) {
          this.logger.error({
            message: "Error discovering peers",
            depth: currentDepth + 1,
            error: error.message,
          });
        }
      }

      unprocessedServers.forEach((server) => processedServers.add(server));

      this.logger.info({
        message: `Completed depth ${currentDepth + 1} discovery`,
        depth: currentDepth + 1,
        processedInThisDepth: currentServers.length,
        newServersInThisDepth: nextDepthServers.size,
        totalProcessed: processedServers.size,
        totalNewServers: newServers.size,
      });

      currentDepthServers = nextDepthServers;
      if (currentDepthServers.size === 0) {
        this.logger.info({
          message: "No more servers to process at next depth",
          stoppingAtDepth: currentDepth + 1,
        });
        break;
      }
    }

    const discoveredServers = Array.from(newServers);
    this.logger.info({
      message: "Server discovery completed",
      totalServersDiscovered: discoveredServers.length,
      totalServersProcessed: processedServers.size,
      finalDepthReached: processedServers.size > 0,
    });

    return discoveredServers;
  }

  async fetchPeers(servers) {
    const batchSize = 20;
    const allPeers = new Set();
    const serverArray = Array.isArray(servers) ? servers : [servers];

    for (let i = 0; i < serverArray.length; i += batchSize) {
      const batch = serverArray.slice(i, i + batchSize);

      const batchResults = await Promise.all(
        batch.map(async (server) => {
          const endpoints = [
            `https://${server}/api/v1/instance/peers`,
            `https://${server}/api/v1/federation/peers`,
          ];

          for (const endpoint of endpoints) {
            try {
              const response = await this.fetchWithBackoff(endpoint);
              const peers = await response.json();
              const peerList = Array.isArray(peers)
                ? peers
                : Object.keys(peers);

              this.logger.info({
                message: "Fetched peers for server",
                server,
                endpoint,
                peersFound: peerList.length,
              });

              return peerList;
            } catch (error) {
              this.logger.debug(
                `Failed to fetch peers from ${endpoint}: ${error.message}`
              );
              continue;
            }
          }
          return [];
        })
      );

      batchResults.flat().forEach((peer) => allPeers.add(peer));
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    return Array.from(allPeers);
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

  async startDiscovery(batchId = null) {
    batchId =
      batchId || `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    const batchSize = 50;
    this.concurrentRequests = 20; // Increased from 5

    try {
      const servers = await this.discoverNewServers(2); // Reduced depth
      const batches = [];

      for (let i = 0; i < servers.length; i += batchSize) {
        batches.push(servers.slice(i, i + batchSize));
      }

      for (const batch of batches) {
        const processingPromises = batch.map(async (domain) => {
          try {
            // Skip if already checked and confirmed non-Korean
            const existingServer = await this.getServerFromDb(domain);
            if (existingServer && existingServer.is_korean_server === false) {
              return;
            }

            // Quick Korean check before detailed processing
            const isKorean = await this.isKoreanInstance(domain);
            if (!isKorean) {
              await this.updateServerInDb({
                domain,
                isActive: true,
                isKoreanServer: false,
                koreanUsageRate: this.koreanUsageRate,
                lastChecked: new Date(),
              });
              return;
            }

            // Proceed with detailed server info gathering for Korean servers
            const nodeInfo = await this.fetchNodeInfo(domain);
            const instanceInfo = await this.fetchInstanceInfo(domain);
            const { isPersonal, instanceType } =
              await this.identifyInstanceType(domain, instanceInfo, nodeInfo);

            await this.updateServerInDb({
              domain,
              isActive: true,
              isKoreanServer: true,
              koreanUsageRate: this.koreanUsageRate,
              ...this.extractServerInfo(instanceInfo),
              hasNodeInfo: !!nodeInfo,
              isPersonalInstance: isPersonal,
              instanceType,
            });

            this.logger.info({
              message: "Korean server processed",
              domain,
              koreanUsageRate: this.koreanUsageRate,
              instanceType,
            });
          } catch (error) {
            this.logger.error({
              message: "Error processing server",
              domain,
              error: error.message,
            });
            await this.updateServerFailure(domain);
          }
        });

        await Promise.all(processingPromises);
      }

      await this.updateBatchStatus(batchId, "completed");

      return batchId;
    } catch (error) {
      this.logger.error({
        message: "Discovery process error",
        batchId,
        error: error.message,
      });
      await this.updateBatchStatus(batchId, "failed");
      throw error;
    }
  }

  async insertNewServers(batchId, domains) {
    const query = `
      INSERT INTO yunabuju_servers 
        (domain, discovery_batch_id, discovery_status, discovery_started_at)
      VALUES 
        ($1, $2, 'pending', CURRENT_TIMESTAMP)
      ON CONFLICT (domain) 
      DO UPDATE SET 
        discovery_batch_id = $2,
        discovery_status = CASE
          WHEN yunabuju_servers.discovery_status IN ('failed', 'not_korean')
             OR yunabuju_servers.next_check_at < CURRENT_TIMESTAMP
          THEN 'pending'
          ELSE yunabuju_servers.discovery_status
        END,
        discovery_started_at = CASE
          WHEN yunabuju_servers.discovery_status IN ('failed', 'not_korean')
             OR yunabuju_servers.next_check_at < CURRENT_TIMESTAMP
          THEN CURRENT_TIMESTAMP
          ELSE yunabuju_servers.discovery_started_at
        END
      WHERE yunabuju_servers.discovery_status IS NULL 
         OR yunabuju_servers.discovery_status = 'failed'
         OR yunabuju_servers.next_check_at < CURRENT_TIMESTAMP
    `;

    for (const domain of domains) {
      try {
        await this.pool.query(query, [domain, batchId]);
        this.logger.debug({
          message: "Inserted/updated server for batch",
          domain,
          batchId,
        });
      } catch (error) {
        this.logger.error({
          message: "Error inserting new server",
          domain,
          batchId,
          error: error.message,
        });
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
          WHEN $2 = false THEN NULL  -- 한국어 서버가 아닌 것으로 확인되면 영구적으로 캐시
          ELSE NULL  -- 한국어 서버인 경우도 매번 체크할 필요 없음
        END,
        updated_at = CURRENT_TIMESTAMP
      WHERE domain = $1
    `;

    await this.pool.query(query, [domain, isKorean, koreanUsageRate]);

    this.logger.info({
      message: `Korean server status updated`,
      domain,
      isKorean,
      koreanUsageRate,
      action: isKorean
        ? "marked as Korean server"
        : "marked as non-Korean server",
    });
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

  async updateBatchStatus(batchId, status) {
    const query = `
      UPDATE yunabuju_servers
      SET 
        discovery_status = $2::varchar,
        discovery_completed_at = CASE 
          WHEN $2::varchar IN ('completed', 'failed') THEN CURRENT_TIMESTAMP 
          ELSE discovery_completed_at 
        END,
        next_check_at = CASE
          WHEN $2::varchar = 'completed' THEN CURRENT_TIMESTAMP + INTERVAL '1 day'
          ELSE next_check_at
        END
      WHERE discovery_batch_id = $1
    `;

    try {
      await this.pool.query(query, [batchId, String(status)]);
      this.logger.info({
        message: "Updated batch status",
        batchId,
        status,
      });
    } catch (error) {
      this.logger.error({
        message: "Error updating batch status",
        batchId,
        status,
        error: error.message,
      });
      throw error;
    }
  }

  async getLastUnfinishedBatch() {
    const query = `
      SELECT 
        discovery_batch_id,
        COUNT(*) as total_servers,
        COUNT(*) FILTER (WHERE discovery_status = 'pending') as pending_servers,
        MIN(discovery_started_at) as started_at,
        MAX(discovery_completed_at) as last_completion,
        STRING_AGG(DISTINCT discovery_status, ', ') as current_statuses
      FROM yunabuju_servers
      WHERE discovery_status = 'pending'
        OR (
          discovery_status = 'in_progress'
          AND discovery_started_at > NOW() - INTERVAL '1 day'
        )
      GROUP BY discovery_batch_id
      ORDER BY started_at DESC
      LIMIT 1
    `;

    try {
      const { rows } = await this.pool.query(query);
      if (rows.length === 0) {
        return null;
      }

      // 추가 정보 포함
      const batch = rows[0];
      batch.age = new Date() - new Date(batch.started_at);
      batch.is_stalled = batch.age > 3600000; // 1시간 이상 진행 중이면 stalled로 간주

      return batch;
    } catch (error) {
      this.logger.error({
        message: "Error fetching unfinished batch",
        error: error.message,
      });
      throw error;
    }
  }

  async resumeDiscovery(batchId = null) {
    if (!batchId) {
      const lastBatch = await this.getLastUnfinishedBatch();
      if (!lastBatch) {
        this.logger.info("No unfinished batch found to resume");
        return false;
      }
      batchId = lastBatch.discovery_batch_id;

      // 오래된 배치는 새로 시작
      if (lastBatch.is_stalled) {
        this.logger.warn({
          message: "Found stalled batch, starting fresh",
          batchId: lastBatch.discovery_batch_id,
          age: lastBatch.age,
        });
        return await this.startDiscovery();
      }

      this.logger.info({
        message: "Found unfinished batch to resume",
        batchId,
        totalServers: lastBatch.total_servers,
        pendingServers: lastBatch.pending_servers,
        startedAt: lastBatch.started_at,
      });
    }

    await this.startDiscovery(batchId);
    return true;
  }
}
