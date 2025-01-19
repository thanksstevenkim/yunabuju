import fetch from "node-fetch";

export class KoreanActivityPubDiscovery {
  constructor(dbPool, logger) {
    this.pool = dbPool;
    this.logger = logger;
    this.seedServers = ["mustard.blog"];
  }

  async fetchWithBackoff(url, attempts = 3) {
    for (let i = 0; i < attempts; i++) {
      try {
        const response = await fetch(url, {
          headers: {
            "User-Agent": "Korean-ActivityPub-Directory/1.0",
          },
        });
        if (!response.ok)
          throw new Error(`HTTP error! status: ${response.status}`);
        return response;
      } catch (error) {
        if (i === attempts - 1) throw error;
        await new Promise((resolve) =>
          setTimeout(resolve, 1000 * Math.pow(2, i))
        );
      }
    }
  }

  async isServerActive(domain) {
    try {
      const response = await this.fetchWithBackoff(
        `https://${domain}/api/v1/instance`
      );
      if (!response.ok) return false;

      // NodeInfo 접근 가능 여부 확인
      const nodeInfo = await this.fetchNodeInfo(domain);
      if (!nodeInfo) return false;

      // 최근 활동 확인
      const recentPosts = await this.fetchRecentPosts(domain);
      const hasRecentActivity = recentPosts && recentPosts.length > 0;

      return hasRecentActivity;
    } catch (error) {
      this.logger.error(`Error checking server status ${domain}:`, error);
      return false;
    }
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

  async checkKoreanSupport(domain, nodeInfo) {
    try {
      if (nodeInfo.metadata?.languages?.includes("ko")) {
        return true;
      }

      const instanceInfo = await this.fetchInstanceInfo(domain);

      if (instanceInfo.languages?.includes("ko")) {
        return true;
      }

      const hasKoreanDescription = this.containsKorean(
        instanceInfo.description
      );
      const hasKoreanRules = instanceInfo.rules?.some((rule) =>
        this.containsKorean(rule.text)
      );

      return hasKoreanDescription || hasKoreanRules;
    } catch (error) {
      return false;
    }
  }

  containsKorean(text) {
    if (!text) return false;
    const koreanRegex =
      /[\uAC00-\uD7AF\u1100-\u11FF\u3130-\u318F\uA960-\uA97F\uD7B0-\uD7FF]/;
    return koreanRegex.test(text);
  }

  async fetchInstanceInfo(domain) {
    try {
      const response = await this.fetchWithBackoff(
        `https://${domain}/api/v1/instance`
      );
      return await response.json();
    } catch (error) {
      return {};
    }
  }

  async fetchRecentPosts(domain) {
    try {
      const response = await this.fetchWithBackoff(
        `https://${domain}/api/v1/timelines/public?local=true&limit=20`
      );
      return await response.json();
    } catch (error) {
      return [];
    }
  }

  async analyzeKoreanUsage(domain) {
    try {
      const posts = await this.fetchRecentPosts(domain);
      if (!posts.length) return 0;

      const koreanPosts = posts.filter((post) =>
        this.containsKorean(post.content)
      );

      return koreanPosts.length / posts.length;
    } catch (error) {
      return 0;
    }
  }

  async isKoreanInstance(domain) {
    try {
      const isActive = await this.isServerActive(domain);
      if (!isActive) return false;

      const nodeInfo = await this.fetchNodeInfo(domain);
      if (!nodeInfo) return false;

      const isKoreanServer = await this.checkKoreanSupport(domain, nodeInfo);

      if (isKoreanServer) {
        const koreanUsageRate = await this.analyzeKoreanUsage(domain);
        return koreanUsageRate > 0.3; // 30% 이상이 한국어 사용자일 경우
      }

      return false;
    } catch (error) {
      this.logger.error(`Error checking Korean instance ${domain}:`, error);
      return false;
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

        // 서버당 1초 간격
        await new Promise((resolve) => setTimeout(resolve, 1000));
      } catch (error) {
        this.logger.error(`Error discovering from ${server}:`, error);
      }
    }

    return Array.from(newServers);
  }

  async fetchPeers(server) {
    try {
      const endpoints = [
        `https://${server}/api/v1/instance/peers`,
        `https://${server}/api/v1/federation/peers`,
      ];

      for (const endpoint of endpoints) {
        try {
          const response = await this.fetchWithBackoff(endpoint);
          if (response.ok) {
            const peers = await response.json();
            return Array.isArray(peers) ? peers : Object.keys(peers);
          }
        } catch (error) {
          continue;
        }
      }

      return [];
    } catch (error) {
      this.logger.error(`Error fetching peers from ${server}:`, error);
      return [];
    }
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
        (domain, is_active, korean_usage_rate, last_checked)
      VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
      ON CONFLICT (domain) 
      DO UPDATE SET 
        is_active = $2,
        korean_usage_rate = $3,
        last_checked = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    `;

    await this.pool.query(query, [
      server.domain,
      server.isActive,
      server.koreanUsageRate,
    ]);
  }

  async getKnownServers() {
    const result = await this.pool.query(
      "SELECT * FROM yunabuju_servers WHERE is_active = true ORDER BY korean_usage_rate DESC"
    );
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
