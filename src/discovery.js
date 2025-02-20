import fetch from "node-fetch";
import https from "https";
import { performance } from "perf_hooks";
import suspiciousDomainsData from "../suspicious-domains.json" assert { type: "json" };

const { suspiciousDomains } = suspiciousDomainsData;

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

  // discovery.js의 processPendingServers 메서드 수정
  async processPendingServers(batchId) {
    const BATCH_SIZE = 100; // 한 번에 처리할 서버 수
    const CONCURRENT_CHECKS = 20; // 동시에 처리할 서버 수
    const CHECK_INTERVAL = 50; // 배치 간 대기 시간 (ms)
    try {
      // 전체 pending 서버 수 확인 쿼리 수정 - failed_attempts >= 3인 서버 제외
      const totalQuery = `
        SELECT COUNT(*) as total 
        FROM yunabuju_servers 
        WHERE discovery_batch_id = $1 
          AND discovery_status = 'pending'
          AND (failed_attempts < 3 OR failed_attempts IS NULL)
      `;
      const {
        rows: [{ total }],
      } = await this.pool.query(totalQuery, [batchId]);

      this.logger.info({
        message: "Starting pending servers processing",
        batchId,
        totalPendingServers: total,
        batchSize: BATCH_SIZE,
        concurrentChecks: CONCURRENT_CHECKS,
        timestamp: new Date().toISOString(),
      });

      let processedCount = 0;

      while (true) {
        // BATCH_SIZE만큼의 pending 서버를 가져오는 쿼리 수정 - failed_attempts >= 3인 서버 제외
        const query = `
          SELECT domain 
          FROM yunabuju_servers 
          WHERE discovery_batch_id = $1 
            AND discovery_status = 'pending'
            AND (failed_attempts < 3 OR failed_attempts IS NULL)
          LIMIT $2
        `;
        const { rows: pendingServers } = await this.pool.query(query, [
          batchId,
          BATCH_SIZE,
        ]);

        if (pendingServers.length === 0) break;

        // CONCURRENT_CHECKS만큼씩 동시 처리
        for (let i = 0; i < pendingServers.length; i += CONCURRENT_CHECKS) {
          const batch = pendingServers.slice(i, i + CONCURRENT_CHECKS);

          this.logger.info({
            message: "Processing batch",
            batchId,
            currentBatch: processedCount + i + 1,
            batchSize: batch.length,
            totalProcessed: processedCount + i,
            totalPending: total,
            percentComplete: (((processedCount + i) / total) * 100).toFixed(2),
          });

          await Promise.all(
            batch.map(async (row) => {
              try {
                // 스킵해야 하는 서버인지 확인
                if (await this.shouldSkipServer(row.domain)) {
                  this.logger.info({
                    message: "Skipping blocked/failed server",
                    domain: row.domain,
                    batchId,
                  });
                  return;
                }

                // 상태를 in_progress로 업데이트
                await this.updateServerStatus(
                  row.domain,
                  batchId,
                  "in_progress"
                );

                this.logger.info({
                  message: "Processing server",
                  domain: row.domain,
                  status: "in_progress",
                });

                const isKorean = await this.isKoreanInstance(row.domain);

                if (!isKorean) {
                  await this.updateServerInDb({
                    domain: row.domain,
                    isActive: true,
                    isKoreanServer: false,
                    koreanUsageRate: this.koreanUsageRate,
                    lastChecked: new Date(),
                    discovery_status: "not_korean",
                  });

                  this.logger.info({
                    message: "Server processed - Not Korean",
                    domain: row.domain,
                    koreanUsageRate: this.koreanUsageRate,
                  });

                  await this.updateKoreanServerStatus(domain, false);
                  return;
                }

                const nodeInfo = await this.fetchNodeInfo(row.domain);
                const instanceInfo = await this.fetchInstanceInfo(row.domain);
                const { isPersonal, instanceType } =
                  await this.identifyInstanceType(
                    row.domain,
                    instanceInfo,
                    nodeInfo
                  );

                await this.updateServerInDb({
                  domain: row.domain,
                  isActive: true,
                  isKoreanServer: true,
                  koreanUsageRate: this.koreanUsageRate,
                  ...this.extractServerInfo(instanceInfo),
                  hasNodeInfo: !!nodeInfo,
                  isPersonalInstance: isPersonal,
                  instanceType,
                  discovery_status: "completed",
                });

                this.logger.info({
                  message: "Server processed - Korean",
                  domain: row.domain,
                  koreanUsageRate: this.koreanUsageRate,
                  instanceType,
                  isPersonal,
                });
              } catch (error) {
                this.logger.error({
                  message: "Error processing server",
                  domain: row.domain,
                  error: error.message,
                });

                await this.updateServerInDb({
                  domain: row.domain,
                  isActive: false,
                  discovery_status: "failed",
                });
              }
            })
          );

          // 배치 간 약간의 대기 시간
          await new Promise((resolve) => setTimeout(resolve, CHECK_INTERVAL));
        }

        processedCount += pendingServers.length;

        this.logger.info({
          message: "Batch complete",
          batchId,
          processedCount,
          totalPending: total,
          percentComplete: ((processedCount / total) * 100).toFixed(2),
        });
      }

      this.logger.info({
        message: "All pending servers processed",
        batchId,
        totalProcessed: processedCount,
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      this.logger.error({
        message: "Error in processPendingServers",
        batchId,
        error: error.message,
      });
      throw error;
    }
  }

  async updateServerStatus(domain, batchId, status) {
    const validStatuses = [
      "pending",
      "in_progress",
      "completed",
      "failed",
      "not_korean",
      "suspicious", // 추가
      "blocked", // 추가
    ];
    if (!validStatuses.includes(status)) {
      throw new Error(`Invalid status: ${status}`);
    }

    const query = `
      UPDATE yunabuju_servers 
      SET 
        discovery_status = $3::text,  -- 명시적 타입 캐스팅 추가
        discovery_completed_at = CASE 
          WHEN $3::text IN ('completed', 'failed', 'not_korean') THEN CURRENT_TIMESTAMP 
          ELSE discovery_completed_at 
        END,
        last_checked = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
      WHERE domain = $1 AND discovery_batch_id = $2
      RETURNING discovery_status
    `;

    try {
      const result = await this.pool.query(query, [
        domain,
        batchId,
        String(status), // 명시적으로 문자열 변환
      ]);

      this.logger.debug({
        message: "Server status updated",
        domain,
        batchId,
        status,
        previousStatus: result.rows[0].discovery_status,
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

  async processWithTimeout(domain, fn, timeout) {
    let timeoutId;
    try {
      return await Promise.race([
        fn(),
        new Promise((_, reject) => {
          timeoutId = setTimeout(() => reject(new Error("Timeout")), timeout);
        }),
      ]);
    } finally {
      clearTimeout(timeoutId);
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
    let registrationType = "open";
    let matchedDescription = null;

    try {
      // 1. 개인 서버 키워드 정의 - 함수 내부로 이동
      const personalKeywords = [
        "개인적",
        "개인",
        "개인용",
        "1인",
        "비공개",
        "지인",
        "personal",
        "private",
        "single user",
        "my own",
        "friends only",
      ];

      // Software type check
      const softwareType = (
        instanceInfo?.software?.name ||
        nodeInfo?.software?.name ||
        ""
      ).toLowerCase();

      // Registration status check
      const registrationClosed =
        instanceInfo?.registrations?.enabled === false ||
        nodeInfo?.openRegistrations === false;

      // Approval check based on software type
      const approvalRequired =
        softwareType === "misskey" || softwareType === "cherrypick"
          ? !nodeInfo?.openRegistrations
          : instanceInfo?.registrations?.approval_required === true;

      if (registrationClosed) {
        registrationType = "closed";
      } else if (approvalRequired) {
        registrationType = "approval_required";
      }

      this.logger.debug({
        message: "Registration status check",
        domain,
        registrationClosed,
        approvalRequired,
        registrationType,
        instanceInfo: {
          enabled: instanceInfo?.registrations?.enabled,
          approval_required: instanceInfo?.registrations?.approval_required,
        },
      });

      // 3. 설명에서 키워드 체크
      const descriptions = [
        { text: instanceInfo?.short_description, source: "short_description" },
        { text: instanceInfo?.description, source: "description" },
        {
          text: nodeInfo?.metadata?.nodeDescription,
          source: "nodeinfo_description",
        },
        { text: nodeInfo?.metadata?.nodeName, source: "nodeinfo_name" },
      ];

      // 설명에서 개인 서버 키워드 체크
      for (const desc of descriptions) {
        if (!desc.text) continue;
        const descText = desc.text.toLowerCase();

        for (const keyword of personalKeywords) {
          if (descText.includes(keyword)) {
            isPersonal = true;
            matchedDescription = {
              source: desc.source,
              keyword,
              text: desc.text,
            };
            break;
          }
        }
        if (isPersonal) break;
      }

      // 4. 추가 조건들 체크
      const userCount = instanceInfo?.stats?.user_count || 0;

      // NodeInfo의 singleUser 체크
      if (!isPersonal && nodeInfo?.metadata?.singleUser === true) {
        isPersonal = true;
        matchedDescription = {
          source: "nodeinfo",
          keyword: "singleUser",
          text: "nodeinfo.metadata.singleUser === true",
        };
      }
      // 폐쇄적이고 소수 사용자
      else if (!isPersonal && registrationClosed && userCount <= 5) {
        isPersonal = true;
        matchedDescription = {
          source: "registration_and_users",
          keyword: "closed_registration",
          text: `closed registration with ${userCount} users`,
        };
      }
      // 최소 사용자 수
      else if (!isPersonal && userCount <= 2) {
        isPersonal = true;
        matchedDescription = {
          source: "user_count",
          keyword: "minimal_users",
          text: `only ${userCount} users`,
        };
      }

      instanceType = isPersonal ? "personal" : "community";

      return {
        isPersonal,
        instanceType,
        matchedDescription,
        registrationType,
        userCount,
        registrationClosed,
        approvalRequired,
      };
    } catch (error) {
      this.logger.error({
        message: "Error identifying instance type",
        domain,
        error: error.message,
      });
      return {
        isPersonal: false,
        instanceType: "community", // 에러 시 기본값을 community로
        matchedDescription: null,
        registrationType: "open",
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
    if (suspiciousDomains.includes(domain)) {
      await this.markServerAsSuspicious(
        domain,
        "listed_in_suspicious_domains",
        "Domain is listed in suspicious-domains.json"
      );
      return false;
    }

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

      // 인스턴스 유형 먼저 확인 - registrationType도 포함
      const { isPersonal, instanceType, registrationType } =
        await this.identifyInstanceType(domain, instanceInfo, nodeInfo);

      // 개인 인스턴스로 확인되면 더 이상 진행하지 않음
      if (isPersonal) {
        await this.updateServerInDb({
          domain,
          isActive: true,
          isPersonalInstance: true,
          instanceType: "personal",
          registrationType, // 추가
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
        registrationType, // 추가
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

  isAllowedContentType(contentType) {
    if (!contentType) return true; // content type이 없으면 허용

    // content type에서 기본 타입만 추출 (파라미터 제거)
    const contentTypeBase = contentType.split(";")[0].trim().toLowerCase();

    // +json 으로 끝나는 모든 타입 허용
    if (contentTypeBase.endsWith("+json")) return true;

    // application/xxx-json 형태의 타입 허용
    if (
      contentTypeBase.startsWith("application/") &&
      contentTypeBase.endsWith("-json")
    )
      return true;

    // 기본 허용 타입들
    const allowedTypes = [
      "application/json",
      "application/activity+json",
      "application/ld+json",
      "text/html",
      "text/plain",
    ];

    // 허용된 타입 목록과 비교
    return allowedTypes.some((type) => contentTypeBase === type.toLowerCase());
  }

  async fetchWithBackoff(url, options = {}, attempts = 3) {
    const parsedUrl = new URL(url);
    const TIMEOUT = 5000;
    const MAX_RETRIES = 2;
    const MAX_RESPONSE_SIZE = 1024 * 1024; // 1MB limit

    const agent = new https.Agent({
      rejectUnauthorized: false,
      servername: parsedUrl.hostname,
      keepAlive: true,
      timeout: TIMEOUT,
      ALPNProtocols: ["http/1.1"],
      minVersion: "TLSv1.2",
      maxVersion: "TLSv1.3",
    });

    for (let i = 0; i < attempts; i++) {
      let timeoutId;
      try {
        const controller = new AbortController();
        timeoutId = setTimeout(() => controller.abort(), TIMEOUT);

        const headers = {
          "User-Agent": "Yunabuju-ActivityPub-Directory/1.0",
          ...options.headers,
        };

        // HEAD 요청을 GET 요청으로 대체
        try {
          const response = await fetch(url, {
            method: "GET",
            headers,
            signal: controller.signal,
            agent,
          });

          const contentLength = parseInt(
            response.headers.get("content-length")
          );
          const contentType = response.headers.get("content-type");

          // contentLength가 유효한 숫자인 경우에만 데이터베이스에 저장
          if (!isNaN(contentLength)) {
            await this.pool.query(
              `
              UPDATE yunabuju_servers 
              SET 
                last_response_size = $2,
                last_content_type = $3,
                updated_at = CURRENT_TIMESTAMP
              WHERE domain = $1
            `,
              [parsedUrl.hostname, contentLength, contentType]
            );

            // Check response size
            if (contentLength > MAX_RESPONSE_SIZE) {
              await this.markServerAsSuspicious(
                parsedUrl.hostname,
                "large_response",
                contentLength
              );
              throw new Error(`Response too large: ${contentLength} bytes`);
            }
          } else {
            // contentLength가 유효하지 않은 경우 contentType만 업데이트
            await this.pool.query(
              `
              UPDATE yunabuju_servers 
              SET 
                last_content_type = $2,
                updated_at = CURRENT_TIMESTAMP
              WHERE domain = $1
            `,
              [parsedUrl.hostname, contentType]
            );
          }

          // Content type check
          if (contentType && !this.isAllowedContentType(contentType)) {
            this.logger.warn({
              message: "Unexpected content type",
              domain: parsedUrl.hostname,
              contentType,
              details:
                "Server will continue to be processed but with a warning",
            });
          }

          return response;
        } catch (error) {
          this.logger.debug(
            `Request failed for ${parsedUrl.hostname}: ${error.message}`
          );
          throw error;
        }

        // ...existing code for main request...
      } catch (error) {
        const isLastAttempt = i === attempts - 1;

        if (error.code === "ENOTFOUND" || error.code === "EAI_AGAIN") {
          await this.updateServerFailure(parsedUrl.hostname);
          if (isLastAttempt) {
            throw new Error(`DNS lookup failed for ${parsedUrl.hostname}`);
          }
        } else if (error.name === "AbortError") {
          await this.updateServerFailure(parsedUrl.hostname);
          if (isLastAttempt) {
            throw new Error(
              `Timeout after ${attempts} attempts for ${parsedUrl.hostname}`
            );
          }
        } else if (isLastAttempt) {
          throw error;
        }

        // Exponential backoff
        const delay = Math.min(1000 * Math.pow(2, i), 5000);
        await new Promise((resolve) => setTimeout(resolve, delay));
      } finally {
        clearTimeout(timeoutId);
      }
    }

    return null;
  }

  async validateSeedServers() {
    const validSeeds = [];

    for (const server of this.seedServers) {
      try {
        this.logger.info({
          message: "Validating seed server",
          server,
          timestamp: new Date().toISOString(),
        });

        const isValid = await Promise.race([
          this.checkServerConnectivity(server),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error("Validation timeout")), 3000)
          ),
        ]);

        if (isValid) {
          validSeeds.push(server);
          this.logger.info({
            message: "Seed server validated successfully",
            server,
          });
        } else {
          this.logger.warn({
            message: "Seed server validation failed",
            server,
            reason: "Server unreachable",
          });
        }
      } catch (error) {
        this.logger.error({
          message: "Seed server validation error",
          server,
          error: error.message,
        });
      }
    }

    if (validSeeds.length === 0) {
      throw new Error("No valid seed servers found");
    }

    this.seedServers = validSeeds;
    return validSeeds;
  }

  async checkServerConnectivity(domain) {
    try {
      const response = await this.fetchWithBackoff(
        `https://${domain}/.well-known/nodeinfo`,
        {},
        1
      );
      return response !== null;
    } catch {
      return false;
    }
  }

  async streamToBuffer(stream) {
    const chunks = [];
    for await (const chunk of stream) {
      chunks.push(chunk);
    }
    return Buffer.concat(chunks);
  }

  async markServerAsSuspicious(domain, reason, details) {
    const query = `
      UPDATE yunabuju_servers 
      SET 
        is_active = false,
        failed_attempts = COALESCE(failed_attempts, 0) + 1,
        last_failed_at = CURRENT_TIMESTAMP,
        suspicious_reason = $2,
        suspicious_details = $3,
        blocked_until = CURRENT_TIMESTAMP + INTERVAL '7 days',
        next_check_at = CURRENT_TIMESTAMP + INTERVAL '7 days',
        discovery_status = 'blocked',
        updated_at = CURRENT_TIMESTAMP
      WHERE domain = $1
    `;

    try {
      await this.pool.query(query, [domain, reason, details]);
      this.logger.warn({
        message: "Server marked as suspicious",
        domain,
        reason,
        details,
      });
    } catch (error) {
      this.logger.error({
        message: "Error marking server as suspicious",
        domain,
        reason,
        error: error.message,
      });
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
      if (nodeInfo) {
        // 디버그 로깅 추가
        this.logger.debug({
          message: "Checking Korean support",
          domain,
          metadata: nodeInfo.metadata,
          containsKoreanInNodeName: this.containsKorean(
            nodeInfo.metadata?.nodeName
          ),
          containsKoreanInNodeDesc: this.containsKorean(
            nodeInfo.metadata?.nodeDescription
          ),
        });

        // 언어 목록 체크
        if (nodeInfo.metadata?.languages?.includes("ko")) {
          this.logger.debug({
            message: "Found Korean in languages list",
            domain,
            languages: nodeInfo.metadata.languages,
          });
          return true;
        }

        // NodeInfo의 텍스트 필드들 체크
        const nodeInfoTexts = [
          nodeInfo.metadata?.nodeName,
          nodeInfo.metadata?.nodeDescription,
          nodeInfo.metadata?.name,
          nodeInfo.metadata?.description,
          nodeInfo.metadata?.maintainer?.name,
          nodeInfo.metadata?.shortDescription,
        ].filter(Boolean);

        for (const text of nodeInfoTexts) {
          if (this.containsKorean(text)) {
            this.logger.debug({
              message: "Found Korean text in nodeinfo",
              domain,
              text,
            });
            return true;
          }
        }
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
      this.logger.error({
        message: "Error in checkKoreanSupport",
        domain,
        error: error.message,
      });
      return false;
    }
  }

  async fetchNodeInfo(domain) {
    try {
      // 1. .well-known/nodeinfo에서 실제 NodeInfo 엔드포인트 URL 가져오기
      const wellKnownResponse = await this.fetchWithBackoff(
        `https://${domain}/.well-known/nodeinfo`
      );

      if (!wellKnownResponse) {
        // 직접 nodeinfo 버전별 URL 시도
        for (const version of ["2.0", "2.1"]) {
          try {
            const directResponse = await this.fetchWithBackoff(
              `https://${domain}/nodeinfo/${version}`
            );
            if (directResponse) {
              const data = await directResponse.json();
              this.logger.debug({
                message: "Found nodeinfo via direct URL",
                domain,
                version,
              });
              return data;
            }
          } catch (error) {
            continue;
          }
        }
        return null;
      }

      const wellKnownData = await wellKnownResponse.json();
      if (!wellKnownData.links) return null;

      // 2. nodeinfo 2.0 또는 2.1 링크 찾기
      const nodeInfoLink = wellKnownData.links.find((link) =>
        link.rel.includes("nodeinfo.diaspora.software/ns/schema/2")
      );

      if (!nodeInfoLink?.href) return null;

      // 3. 실제 NodeInfo 데이터 가져오기
      const nodeInfoResponse = await this.fetchWithBackoff(nodeInfoLink.href);
      if (!nodeInfoResponse) return null;

      const nodeInfoData = await nodeInfoResponse.json();

      // 디버그 로깅 추가
      this.logger.debug({
        message: "Fetched nodeinfo data",
        domain,
        metadata: nodeInfoData.metadata,
        languages: nodeInfoData.metadata?.languages,
        nodeName: nodeInfoData.metadata?.nodeName,
        nodeDescription: nodeInfoData.metadata?.nodeDescription,
      });

      return nodeInfoData;
    } catch (error) {
      this.logger.debug(
        `Error fetching nodeinfo from ${domain}: ${error.message}`
      );
      return null;
    }
  }

  async fetchInstanceInfo(domain) {
    try {
      // 모든 서버 유형의 기본 엔드포인트 먼저 시도
      const commonEndpoints = [
        { path: "/api/v1/instance", method: "GET" },
        { path: "/api/instance", method: "GET" },
      ];

      // Mastodon/Pleroma API 먼저 시도
      for (const endpoint of commonEndpoints) {
        try {
          const response = await this.fetchWithBackoff(
            `https://${domain}${endpoint.path}`,
            { method: endpoint.method }
          );
          if (!response) continue;
          const data = await response.json();
          if (data && (data.title || data.uri || data.description)) {
            this.logger.debug({
              message: "Instance info found via API",
              domain,
              endpoint: endpoint.path,
              title: data.title,
              description: data.description,
            });
            return this.processInstanceData(data);
          }
        } catch (error) {
          if (error.message.includes("422")) continue;
          this.logger.debug({
            message: "API endpoint failed",
            domain,
            endpoint: endpoint.path,
            error: error.message,
          });
        }
      }

      // Misskey API 시도
      try {
        const response = await this.fetchWithBackoff(
          `https://${domain}/api/meta`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({}),
          }
        );
        if (response) {
          const data = await response.json();
          if (data) return this.processInstanceData(data, "misskey");
        }
      } catch (error) {
        if (error.message.includes("422")) {
          const nodeInfo = await this.fetchNodeInfo(domain);
          if (nodeInfo) return this.processNodeInfo(nodeInfo);
        }
      }

      throw new Error("서버 정보 조회 실패");
    } catch (error) {
      throw error;
    }
  }

  processInstanceData(data, type = "mastodon") {
    const registrations = {
      enabled:
        type === "misskey"
          ? !data.disableRegistration
          : data.registrations?.enabled ?? !data.closed,
      approval_required:
        type === "mastodon" || type === "pleroma"
          ? data.registrations?.approval_required ?? false
          : false, // Misskey, Cherrypick 등은 nodeinfo에서만 확인 가능
    };

    return {
      software: {
        name: type,
        version: data.version,
      },
      languages: data.languages || [],
      stats: {
        user_count: data.stats?.users || data.stats?.user_count || 0,
      },
      title: data.title || "",
      description: data.description || "",
      rules: data.rules || [],
      registrations,
    };
  }

  processNodeInfo(nodeInfo) {
    // Software 이름 확인
    const softwareName = (nodeInfo.software?.name || "unknown").toLowerCase();

    // Misskey나 Cherrypick의 경우 NodeInfo의 openRegistrations로 판단
    const approval_required =
      softwareName === "misskey" || softwareName === "cherrypick"
        ? !nodeInfo.openRegistrations // 등록이 닫혀있으면 approval이 필요한 것으로 간주
        : null; // 다른 소프트웨어는 Instance API에서 확인

    return {
      software: {
        name: softwareName,
        version: nodeInfo.software?.version,
      },
      languages: nodeInfo.metadata?.languages || [],
      stats: {
        user_count: nodeInfo.usage?.users?.total || 0,
      },
      description:
        nodeInfo.metadata?.nodeDescription || nodeInfo.metadata?.nodeName || "",
      extended_description: nodeInfo.metadata?.nodeDescription || "",
      branding_server_description: nodeInfo.metadata?.nodeName || "",
      rules: [],
      registrations: {
        enabled: nodeInfo.openRegistrations !== false,
        approval_required: approval_required,
      },
    };
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
      extended_description: nodeInfo.metadata?.nodeDescription || "",
      branding_server_description: nodeInfo.metadata?.nodeName || "",
      rules: [],
    };
  }

  stripHtml(html) {
    return html.replace(/<[^>]*>/g, "");
  }

  // 타임라인 데이터 가져오기
  async fetchTimelineData(domain) {
    // Misskey 체크를 위한 간단한 엔드포인트 시도
    try {
      const response = await this.fetchWithBackoff(
        `https://${domain}/api/meta`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({}),
        }
      );
      if (response) {
        // Misskey 엔드포인트
        const timelineResponse = await this.fetchWithBackoff(
          `https://${domain}/api/notes/local-timeline`,
          {
            method: "POST",
            body: JSON.stringify({ limit: 50, withFiles: false }),
          }
        );
        if (!timelineResponse) return null;
        const data = await timelineResponse.json();
        return { data, softwareName: "misskey" };
      }
    } catch {}

    // 기본 Mastodon/Pleroma 엔드포인트
    const response = await this.fetchWithBackoff(
      `https://${domain}/api/v1/timelines/public?local=true&limit=50`
    );
    if (!response) return null;

    try {
      const data = await response.json();
      return { data, softwareName: "mastodon" };
    } catch {
      return null;
    }
  }

  async withTimeout(promise, ms = 2000) {
    return Promise.race([
      promise,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("Timeout")), ms)
      ),
    ]).catch(() => null);
  }

  analyzeTimelineContent(timelineData) {
    if (!timelineData || !Array.isArray(timelineData.data)) return 0;

    const { data, softwareName } = timelineData;
    const validPosts = data.filter((post) => {
      const content = softwareName === "misskey" ? post.text : post.content;
      return typeof content === "string" && content.length > 0;
    });

    // 최소 5개의 포스트만 있으면 분석 진행 (기존 값보다 낮춤)
    if (validPosts.length < 5) {
      this.logger.debug({
        message: "Insufficient posts for analysis",
        postsFound: validPosts.length,
      });
      return 0;
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

    const rate = koreanPosts.length / validPosts.length;

    this.logger.debug({
      message: "Timeline analysis complete",
      totalPosts: validPosts.length,
      koreanPosts: koreanPosts.length,
      rate: rate,
    });

    return rate;
  }

  extractNextLink(linkHeader) {
    if (!linkHeader) return null;
    const links = linkHeader.split(",");
    const nextLink = links.find((link) => link.includes('rel="next"'));
    if (!nextLink) return null;

    const matches = nextLink.match(/<([^>]+)>/);
    return matches ? matches[1] : null;
  }

  async discoverNewServers(depth = 2) {
    await this.validateSeedServers();

    const processedServers = new Set(this.seedServers);
    const newServers = new Set();
    const koreanServers = new Set();
    const SERVER_TIMEOUT = 5000;

    // 시드서버들의 피어만 수집하고 검사
    for (const seedServer of this.seedServers) {
      try {
        this.logger.info({
          message: "Fetching peers from seed server",
          seedServer,
        });

        const { peers } = await this.fetchPeers(seedServer);

        // 각 피어 검사
        for (const peer of peers) {
          if (processedServers.has(peer)) continue;
          processedServers.add(peer);

          try {
            // 서버 검사
            const existingServer = await this.getServerFromDb(peer);
            if (existingServer) {
              if (existingServer.is_korean_server) {
                koreanServers.add(peer);
                newServers.add(peer);
              }
              continue;
            }

            const isKorean = await this.isKoreanInstance(peer);
            if (isKorean) {
              koreanServers.add(peer);
              newServers.add(peer);
            }
          } catch (error) {
            this.logger.error(`Error processing peer ${peer}:`, error);
          }

          // 각 피어 처리 후 잠시 대기
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      } catch (error) {
        this.logger.error(
          `Error fetching peers from seed server ${seedServer}:`,
          error
        );
      }
    }

    this.logger.info({
      message: "Discovery completed",
      totalProcessed: processedServers.size,
      newServersFound: newServers.size,
      koreanServersFound: koreanServers.size,
    });

    return Array.from(newServers);
  }

  async saveDiscoveryState(state) {
    const query = `
      INSERT INTO yunabuju_discovery_state (
        current_depth, processed_servers, new_servers, 
        last_processed_index, current_peers, current_server,
        updated_at
      ) VALUES ($1, $2::text[], $3::text[], $4, $5::text[], $6, CURRENT_TIMESTAMP)
      ON CONFLICT (id) DO UPDATE SET
        current_depth = $1,
        processed_servers = $2::text[],
        new_servers = $3::text[],
        last_processed_index = $4,
        current_peers = $5::text[],
        current_server = $6,
        updated_at = CURRENT_TIMESTAMP`;

    await this.pool.query(query, [
      state.currentDepth,
      state.processedServers,
      state.newServers,
      state.lastProcessedIndex,
      state.currentPeers || [],
      state.currentServer,
    ]);
  }

  async getDiscoveryState() {
    const query = `
      SELECT * FROM yunabuju_discovery_state 
      WHERE updated_at > NOW() - INTERVAL '1 day'
      ORDER BY updated_at DESC 
      LIMIT 1
    `;

    const result = await this.pool.query(query);
    return result.rows[0];
  }

  async saveDiscoveryProgress(server, lastPeerIndex, currentPeers) {
    const query = `
      UPDATE yunabuju_discovery_state 
      SET 
        last_processed_index = $1,
        current_peers = $2,
        current_server = $3,
        updated_at = CURRENT_TIMESTAMP
      WHERE id = 1
    `;

    await this.pool.query(query, [lastPeerIndex, currentPeers, server]);
  }

  async getDiscoveryProgress() {
    const query = `
      SELECT last_processed_index, current_peers, current_server 
      FROM yunabuju_discovery_state 
      WHERE id = 1
    `;

    const result = await this.pool.query(query);
    return result.rows[0];
  }

  async fetchPeers(servers, lastProcessedIndex = 0) {
    const batchSize = 50;
    const allPeers = new Set();
    const serverArray = Array.isArray(servers) ? servers : [servers];

    const tryAllEndpoints = async (server) => {
      this.logger.info(`Fetching peers from ${server}`);

      try {
        // 먼저 nodeinfo로 서버 타입 확인
        const nodeInfoResponse = await this.fetchWithBackoff(
          `https://${server}/.well-known/nodeinfo`
        );
        const nodeInfoData = await nodeInfoResponse.json();

        let endpoints;
        const nodeInfoLink = nodeInfoData.links.find(
          (link) =>
            link.rel === "http://nodeinfo.diaspora.software/ns/schema/2.0"
        );

        if (nodeInfoLink) {
          const nodeInfo = await (
            await this.fetchWithBackoff(nodeInfoLink.href)
          ).json();
          const software = nodeInfo.software?.name?.toLowerCase();

          if (software === "misskey") {
            endpoints = [
              {
                url: `https://${server}/api/federation/instances`,
                method: "POST",
                body: { limit: 100, offset: 0, sort: "+id" },
              },
            ];
          } else if (software === "mastodon") {
            endpoints = [`https://${server}/api/v1/instance/peers`];
          } else if (software === "pleroma") {
            endpoints = [`https://${server}/api/instance/peers`];
          } else {
            endpoints = [
              `https://${server}/api/v1/instance/peers`,
              `https://${server}/api/instance/peers`,
            ];
          }
        }

        for (const endpoint of endpoints || []) {
          try {
            const url = typeof endpoint === "string" ? endpoint : endpoint.url;
            const response =
              typeof endpoint === "string"
                ? await this.fetchWithBackoff(url)
                : await this.fetchWithBackoff(url, {
                    method: endpoint.method,
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(endpoint.body),
                  });

            const data = await response.json();

            let peers = [];
            if (Array.isArray(data)) peers = data;
            else if (data.instances) peers = data.instances.map((i) => i.host);

            if (peers.length > 0) {
              this.logger.info(`Found ${peers.length} peers from ${url}`);
              return peers;
            }
          } catch (error) {
            this.logger.debug(
              `Failed endpoint ${
                typeof endpoint === "string" ? endpoint : endpoint.url
              }: ${error.message}`
            );
          }
        }
      } catch (error) {
        this.logger.error(
          `Failed to fetch nodeinfo from ${server}: ${error.message}`
        );
      }
      return [];
    };

    for (let i = lastProcessedIndex; i < serverArray.length; i += batchSize) {
      const batch = serverArray.slice(i, i + batchSize);
      const batchResults = await Promise.all(
        batch.map((server) => tryAllEndpoints(server))
      );

      const newPeers = batchResults.flat();
      newPeers.forEach((peer) => allPeers.add(peer));

      this.logger.info(`Found ${newPeers.length} new peers in this batch`);
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    return {
      peers: Array.from(allPeers),
      lastProcessedIndex: serverArray.length,
    };
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
        instance_type, last_checked, discovery_status, node_name, node_description,
        registration_type)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP, $14, $15, $16, $17)
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
        node_name = $15,
        node_description = $16,
        registration_type = $17,
        last_checked = CURRENT_TIMESTAMP,
        discovery_status = COALESCE($14, yunabuju_servers.discovery_status),
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
      server.discovery_status,
      server.nodeName,
      server.nodeDescription,
      server.registrationType || "open", // 추가된 필드
    ]);

    this.logger.debug({
      message: "Server info updated in DB",
      domain: server.domain,
      nodeName: server.nodeName,
      nodeDescription: server.nodeDescription?.substring(0, 50) + "...", // 로깅 시 긴 설명은 잘라서 표시
    });
  }

  async getKnownServers(
    includeClosedRegistration = false,
    excludePersonal = false,
    includeApprovalRequired = false
  ) {
    let query = `
      SELECT * FROM yunabuju_servers 
      WHERE is_active = true AND is_korean_server = true
    `;

    if (!includeClosedRegistration) {
      query += ` AND (registration_type = 'open'`;
      if (includeApprovalRequired) {
        query += ` OR registration_type = 'approval_required'`;
      }
      query += `)`;
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

    try {
      // 시드 서버 초기화
      await this.initializeSeedServers();

      // 새 배치인 경우에만 서버 발견
      if (!batchId.includes("-")) {
        this.logger.info({
          message: "Using existing batch ID",
          batchId,
        });
      } else {
        this.logger.info({
          message: "Starting new discovery batch",
          batchId,
        });
        const servers = await this.discoverNewServers(2);
        await this.insertNewServers(batchId, servers);
      }

      // pending 서버 처리
      await this.processPendingServers(batchId);
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

  async updateKoreanServerStatus(domain, isKorean, koreanUsageRate = 0) {
    // null 대신 0으로 기본값 변경
    const query = `
      UPDATE yunabuju_servers 
      SET 
        is_korean_server = $2,
        korean_usage_rate = $3,
        last_korean_check = CURRENT_TIMESTAMP,
        next_korean_check = CASE
          WHEN $2 = false THEN null
          ELSE CURRENT_TIMESTAMP + INTERVAL '1 day'
        END,
        updated_at = CURRENT_TIMESTAMP
      WHERE domain = $1
    `;

    // koreanUsageRate가 null이면 0으로 처리
    const rate = koreanUsageRate === null ? 0 : koreanUsageRate;

    await this.pool.query(query, [domain, isKorean, rate]);

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
    const startTime = performance.now();

    try {
      this.logger.info({
        message: "Starting Korean server check",
        domain,
        timestamp: new Date().toISOString(),
      });

      // 1. 캐시 체크
      const cachedResult = await this.pool.query(
        "SELECT is_korean_server, korean_usage_rate FROM yunabuju_servers WHERE domain = $1 AND last_korean_check > NOW() - INTERVAL '1 day'",
        [domain]
      );

      if (cachedResult.rows.length > 0) {
        const { is_korean_server, korean_usage_rate } = cachedResult.rows[0];
        this.koreanUsageRate = korean_usage_rate;
        return is_korean_server;
      }

      // 2. 기본 정보 수집 (병렬)
      let [nodeInfo, instanceInfo, timelineData] = await Promise.all([
        this.fetchNodeInfo(domain).catch(() => null),
        this.fetchInstanceInfo(domain).catch(() => null),
        this.fetchTimelineData(domain).catch(() => null),
      ]);

      // 3. 응답 데이터가 전혀 없으면 빠르게 실패
      if (!nodeInfo && !instanceInfo && !timelineData) {
        this.logger.debug({
          message: "No server information available",
          domain,
        });
        await this.updateKoreanServerStatus(domain, false, 0);
        return false;
      }

      // 4. 타임라인 분석
      let koreanRate = 0;
      if (timelineData) {
        koreanRate = this.analyzeTimelineContent(timelineData);
        this.logger.debug({
          message: "Timeline analysis result",
          domain,
          koreanRate,
        });
      }

      // 5. 메타데이터에서 한국어 지원 여부 체크
      const hasKoreanSupport = await this.checkKoreanSupport(
        domain,
        nodeInfo || {}
      );

      // 6. 한국어 서버 판정 조건 수정
      const isKorean =
        (hasKoreanSupport && koreanRate >= 0.15) || // 메타데이터에 한국어가 있고 타임라인 한국어 사용률 15% 이상
        koreanRate >= 0.3; // 또는 타임라인 한국어 사용률이 30% 이상

      if (isKorean) {
        // nodeInfo와 instanceInfo를 이용해서 서버 정보 업데이트
        const serverInfo = this.extractServerInfo(instanceInfo, nodeInfo);
        const { isPersonal, instanceType } = await this.identifyInstanceType(
          domain,
          instanceInfo,
          nodeInfo
        );

        // DB에 전체 정보 업데이트
        await this.updateServerInDb({
          domain,
          isActive: true,
          isKoreanServer: true,
          koreanUsageRate: koreanRate,
          ...serverInfo,
          hasNodeInfo: !!nodeInfo,
          isPersonalInstance: isPersonal,
          instanceType, // 하드코딩된 "community" 대신 실제 판별된 타입 사용
          discovery_status: "completed",
        });

        await this.updateKoreanServerStatus(domain, true, koreanRate);
        return true;
      }

      // 한국어 서버가 아닌 것으로 판단
      await this.updateKoreanServerStatus(domain, false, koreanRate);
      return false;
    } catch (error) {
      const processingTime = performance.now() - startTime;
      this.logger.error({
        message: "Error in Korean server check",
        domain,
        error: error.message,
        processingTime: processingTime.toFixed(2),
      });

      await this.updateKoreanServerStatus(domain, false, 0);
      return false;
    }
  }

  async updateBatchStatus(batchId, status) {
    const query = `
      UPDATE yunabuju_servers
      SET 
        discovery_status = $2::text,  -- 명시적 타입 캐스팅 추가
        discovery_completed_at = CASE 
          WHEN $2::text IN ('completed', 'failed') THEN CURRENT_TIMESTAMP 
          ELSE discovery_completed_at 
        END,
        next_check_at = CASE
          WHEN $2::text = 'completed' THEN CURRENT_TIMESTAMP + INTERVAL '1 day'
          ELSE next_check_at
        END
      WHERE discovery_batch_id = $1
    `;

    try {
      await this.pool.query(query, [batchId, String(status)]); // 명시적으로 문자열 변환
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

  async initializeSeedServers() {
    this.logger.info("Initializing seed servers...");

    for (const domain of this.seedServers) {
      try {
        // 이미 DB에 있는지 확인
        const existingServer = await this.getServerFromDb(domain);
        if (existingServer?.is_active && !existingServer.is_personal_instance) {
          this.logger.info({
            message: "Seed server already in database",
            domain,
            status: "skipping initialization",
          });
          continue;
        }

        this.logger.info({
          message: "Initializing seed server",
          domain,
        });

        // 서버 정보 수집 - 정보가 없어도 계속 진행
        const nodeInfo = await this.fetchNodeInfo(domain).catch(() => null);
        const instanceInfo = await this.fetchInstanceInfo(domain).catch(
          () => null
        );

        const serverInfo = this.extractServerInfo(instanceInfo, nodeInfo);

        // DB에 저장
        await this.updateServerInDb({
          domain,
          isActive: true,
          isKoreanServer: true, // 시드서버는 한국어 서버로 간주
          koreanUsageRate: 1.0, // 시드서버는 최대값으로 설정
          ...serverInfo,
          hasNodeInfo: !!nodeInfo,
          isPersonalInstance: false,
          instanceType: "community",
          discovery_status: "verified_seed",
        });

        this.logger.info({
          message: "Seed server initialized successfully",
          domain,
          serverInfo,
        });
      } catch (error) {
        this.logger.error({
          message: "Failed to initialize seed server",
          domain,
          error: error.message,
          stack: error.stack,
        });
        continue; // 에러가 발생해도 다음 시드서버 처리 계속
      }
    }

    this.logger.info("Seed servers initialization completed");
  }

  async shouldSkipServer(domain) {
    const query = `
      SELECT domain, failed_attempts, blocked_until
      FROM yunabuju_servers
      WHERE domain = $1 AND (
        failed_attempts >= 3 OR
        (blocked_until IS NOT NULL AND blocked_until > CURRENT_TIMESTAMP) OR
        discovery_status = 'blocked'
      )
    `;

    try {
      const { rows } = await this.pool.query(query, [domain]);
      if (rows.length > 0) {
        const server = rows[0];
        this.logger.debug({
          message: "Skipping blocked/failed server",
          domain,
          failedAttempts: server.failed_attempts,
          blockedUntil: server.blocked_until,
        });
        return true;
      }
      return false;
    } catch (error) {
      this.logger.error({
        message: "Error checking server skip status",
        domain,
        error: error.message,
      });
      return false;
    }
  }

  async analyzeKoreanUsage(domain) {
    try {
      const timelineData = await this.fetchTimelineData(domain);
      if (!timelineData) return 0;

      const koreanRate = this.analyzeTimelineContent(timelineData);
      return koreanRate;
    } catch (error) {
      this.logger.debug({
        message: "Error analyzing Korean usage",
        domain,
        error: error.message,
      });
      return 0;
    }
  }

  async cleanupLowUsageServers() {
    const MIN_KOREAN_USAGE_RATE = 0.15;

    try {
      const query = `
        UPDATE yunabuju_servers 
        SET 
          is_korean_server = false,
          discovery_status = 'not_korean',
          updated_at = CURRENT_TIMESTAMP
        WHERE 
          is_korean_server = true 
          AND korean_usage_rate <= $1
        RETURNING domain
      `;

      const result = await this.pool.query(query, [MIN_KOREAN_USAGE_RATE]);
      const cleanedCount = result.rowCount;
      const cleanedDomains = result.rows.map((row) => row.domain);

      this.logger.info({
        message: "Cleaned up low usage Korean servers",
        removedCount: cleanedCount,
        threshold: MIN_KOREAN_USAGE_RATE,
        affectedDomains: cleanedDomains,
      });

      return {
        cleanedCount,
        cleanedDomains,
      };
    } catch (error) {
      this.logger.error({
        message: "Error cleaning up low usage servers",
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  extractServerInfo(instanceInfo, nodeInfo = null) {
    if (!instanceInfo && !nodeInfo) {
      return {
        softwareName: "unknown",
        softwareVersion: "",
        totalUsers: 0,
        description: "",
        nodeName: "",
        nodeDescription: "",
        registrationOpen: null,
        registrationApprovalRequired: null,
      };
    }

    // Get server name using priority order (Instance API 우선)
    const nodeName =
      instanceInfo?.title || // 1. Instance API title (높은 우선순위)
      nodeInfo?.metadata?.nodeName || // 2. NodeInfo name
      nodeInfo?.metadata?.name || // 3. Alternative NodeInfo name
      instanceInfo?.name || // 4. Instance API name
      "Unknown";

    // 로그 추가
    this.logger.debug({
      message: "Server name resolution",
      domain: instanceInfo?.domain || nodeInfo?.domain,
      sources: {
        instanceTitle: instanceInfo?.title,
        nodeInfoName: nodeInfo?.metadata?.nodeName,
        nodeInfoAltName: nodeInfo?.metadata?.name,
        instanceName: instanceInfo?.name,
      },
      finalName: nodeName,
    });

    // Get description using priority order
    const description =
      nodeInfo?.metadata?.nodeDescription || // 1. Primary NodeInfo description
      nodeInfo?.metadata?.description || // 2. Alternative NodeInfo description
      instanceInfo?.description || // 3. Instance API description
      instanceInfo?.short_description || // 4. Instance API short description
      "";

    // Get extended description using priority order
    const nodeDescription =
      nodeInfo?.metadata?.nodeDescription || // 1. Primary NodeInfo description
      instanceInfo?.extended_description || // 2. Instance API extended description
      instanceInfo?.description || // 3. Instance API main description
      instanceInfo?.short_description || // 4. Instance API short description
      description; // 5. Fall back to main description

    this.logger.debug({
      message: "Merging server info",
      domain: instanceInfo?.domain || nodeInfo?.domain,
      nodeInfo: {
        nodeName: nodeInfo?.metadata?.nodeName,
        nodeDescription: nodeInfo?.metadata?.nodeDescription,
      },
      instanceInfo: {
        title: instanceInfo?.title,
        description: instanceInfo?.description,
        shortDescription: instanceInfo?.short_description,
      },
      final: {
        nodeName,
        description,
        nodeDescription,
      },
    });

    return {
      softwareName:
        instanceInfo?.software?.name ||
        nodeInfo?.software?.name?.toLowerCase() ||
        "unknown",
      softwareVersion:
        instanceInfo?.software?.version || nodeInfo?.software?.version || "",
      totalUsers:
        instanceInfo?.stats?.user_count ||
        instanceInfo?.stats?.users?.total ||
        nodeInfo?.usage?.users?.total ||
        0,
      description,
      nodeName,
      nodeDescription,
      registrationOpen:
        instanceInfo?.registrations?.enabled ??
        nodeInfo?.openRegistrations ??
        null,
      registrationApprovalRequired:
        instanceInfo?.registrations?.approval_required ?? null,
    };
  } // 디버그를 위한 새로운 메서드 추가  async debugServerStatus(domain) {    const server = await this.getServerFromDb(domain);    this.logger.debug({      message: "Current server status in DB",      domain,      server,    });    return server;  }
}
