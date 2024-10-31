import { getDB, transaction } from "@hasura/ndc-duckduckapi";
import { ExponentialBackoff } from "./ExponentialBackoff";

export const GithubIssuesSyncSchema = `
        CREATE TABLE IF NOT EXISTS github_issues (
          id bigint PRIMARY KEY,
          number INTEGER,
          is_pull_request BOOLEAN,
          title TEXT,
          body TEXT,
          state TEXT,
          created_at TIMESTAMP,
          updated_at TIMESTAMP,
          closed_at TIMESTAMP,
          user_login TEXT,
          labels TEXT,
          comment_count INTEGER,
          repository TEXT
        );
        CREATE TABLE IF NOT EXISTS github_comments (
          id bigint PRIMARY KEY,
          issue_id bigint,
          body TEXT,
          user_login TEXT,
          created_at TIMESTAMP,
          updated_at TIMESTAMP,
          repository TEXT,
          FOREIGN KEY(issue_id) REFERENCES github_issues(id)
        );

        CREATE TABLE IF NOT EXISTS issue_sync_state (
          repository TEXT PRIMARY KEY,
          last_issue_sync TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS comment_sync_state (
          issue_id bigint PRIMARY KEY,
          repository TEXT,
          last_comment_sync TIMESTAMP,
          FOREIGN KEY(issue_id) REFERENCES github_issues(id)
);
      `;

interface Issue {
  id: bigint;
  number: number;
  pull_request: any;
  title: string;
  body: string;
  state: string;
  created_at: string;
  updated_at: string;
  closed_at: string | null;
  user: {
    login: string;
  };
  labels: Array<{
    name: string;
  }>;
  comments: number;
}

interface IssueComment {
  id: bigint;
  issue_id: bigint;
  user: {
    login: string;
  };
  body: string;
  created_at: string;
  updated_at: string;
}

interface SyncState {
  last_issue_sync: string;
  repository: string;
}

interface IssueCommentSyncState {
  issue_id: bigint;
  last_comment_sync: string;
}

export class GitHubIssueSyncManager {
  private baseUrl = "https://api.github.com";
  private token: string = "";
  private owner: string;
  private repo: string;
  private syncInterval: NodeJS.Timeout | null = null;

  constructor(owner: string, repo: string) {
    this.owner = owner;
    this.repo = repo;
    console.log(`Initializing GitHub sync for ${owner}/${repo}`);
  }

  private async githubFetch(
    endpoint: string,
    params: Record<string, any> = {}
  ): Promise<any> {
    const url = new URL(`${this.baseUrl}${endpoint}`);
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined) {
        url.searchParams.append(key, String(value));
      }
    });

    const response = await fetch(url.toString(), {
      headers: {
        Authorization: `token ${this.token}`,
        Accept: "application/vnd.github.v3+json",
        "User-Agent": "GitHub-Issue-Sync",
      },
    });

    if (!response.ok) {
      const error = await response.json().catch(() => null);
      throw new Error(
        `GitHub API error (${response.status}): ${JSON.stringify(error)}`
      );
    }

    return response.json();
  }

  async initialize(token: string): Promise<void> {
    const backoff = new ExponentialBackoff({
      initialDelay: 5 * 1000,
      maxDelay: 65 * 60 * 1000,
      maxAttempts: 0,
      factor: 2,
      jitter: false,
    });

    await backoff.execute(async () => {
      this.getSyncStatus();
      console.log("Starting initialization...");
      this.token = token;

      // Validate token by fetching user info
      console.log("Validating GitHub token...");
      await this.githubFetch("/user");
      console.log("Token validation successful");

      // Start initial sync
      console.log("Starting initial sync...");
      await this.syncIssuesAndComments();
      console.log("Initial sync complete");

      // Set up continuous sync
      console.log("Setting up continuous sync (5 minute interval)...");
      this.syncInterval = setInterval(
        () => this.syncIssuesAndComments(),
        5 * 60 * 1000
      );
    });
  }

  private async getLastIssueSyncState(): Promise<SyncState | null> {
    const db = await getDB();
    try {
      const result = await db.all(
        `SELECT repository, last_issue_sync 
         FROM issue_sync_state 
         WHERE repository = ?`,
        `${this.owner}/${this.repo}`
      );
      return result.length > 0 ? (result[0] as SyncState) : null;
    } catch (error) {
      console.error("Failed to get issue sync state:", error);
      throw error;
    }
  }

  private async getCommentSyncState(issueId: bigint): Promise<string | null> {
    const db = await getDB();
    try {
      const result = await db.all(
        `SELECT last_comment_sync 
         FROM comment_sync_state 
         WHERE issue_id = ? AND repository = ?`,
        issueId,
        `${this.owner}/${this.repo}`
      );
      return result.length > 0 ? result[0].last_comment_sync : null;
    } catch (error) {
      console.error(
        `Failed to get comment sync state for issue ${issueId}:`,
        error
      );
      throw error;
    }
  }

  private async updateIssueSyncState(): Promise<void> {
    try {
      const db = await getDB();
      await transaction(db, async (conn) => {
        await conn.run(
          `
          INSERT INTO issue_sync_state (repository, last_issue_sync)
          VALUES (?, ?)
          ON CONFLICT(repository) DO UPDATE SET last_issue_sync = ?
        `,
          `${this.owner}/${this.repo}`,
          new Date().toISOString(),
          new Date().toISOString()
        );
      });
      console.log("Issue sync state updated");
    } catch (error) {
      console.error("Failed to update issue sync state:", error);
      throw error;
    }
  }

  private async updateCommentSyncState(issueId: bigint): Promise<void> {
    try {
      const db = await getDB();
      await transaction(db, async (conn) => {
        await conn.run(
          `
          INSERT INTO comment_sync_state (issue_id, repository, last_comment_sync)
          VALUES (?, ?, ?)
          ON CONFLICT(issue_id) DO UPDATE SET last_comment_sync = ?
        `,
          issueId,
          `${this.owner}/${this.repo}`,
          new Date().toISOString(),
          new Date().toISOString()
        );
      });
      console.log(`Comment sync state updated for issue ${issueId}`);
    } catch (error) {
      console.error(
        `Failed to update comment sync state for issue ${issueId}:`,
        error
      );
      throw error;
    }
  }

  private async fetchAllIssues(since?: string): Promise<Issue[]> {
    const issues: Issue[] = [];
    let page = 1;

    try {
      while (true) {
        console.log(`Fetching issues page ${page}...`);
        const params: Record<string, any> = {
          state: "all",
          per_page: 100,
          page,
          sort: "updated",
          direction: "desc",
        };

        if (since) {
          params.since = since;
        }

        const data = await this.githubFetch(
          `/repos/${this.owner}/${this.repo}/issues`,
          params
        );

        if (data.length === 0) break;

        issues.push(...data);
        page++;
      }

      console.log(`Fetched total of ${issues.length} issues`);
      return issues;
    } catch (error) {
      console.error("Failed to fetch issues:", error);
      throw error;
    }
  }

  private async fetchCommentsForIssue(
    issueNumber: number,
    since?: string
  ): Promise<IssueComment[]> {
    const comments: IssueComment[] = [];
    let page = 1;

    try {
      while (true) {
        console.log(
          `Fetching comments page ${page} for issue #${issueNumber}...`
        );
        const params: Record<string, any> = {
          per_page: 100,
          page,
        };

        if (since) {
          params.since = since;
        }

        const data = await this.githubFetch(
          `/repos/${this.owner}/${this.repo}/issues/${issueNumber}/comments`,
          params
        );

        if (data.length === 0) break;

        comments.push(...data);
        page++;
      }

      console.log(
        `Fetched ${comments.length} comments for issue #${issueNumber}`
      );
      return comments;
    } catch (error) {
      console.error(
        `Failed to fetch comments for issue #${issueNumber}:`,
        error
      );
      throw error;
    }
  }

  private async syncIssuesAndComments(): Promise<void> {
    try {
      console.log("Starting sync cycle...");
      const lastIssueSync = await this.getLastIssueSyncState();
      console.log(
        `Last issue sync: ${lastIssueSync?.last_issue_sync || "never"}`
      );

      // First sync issues
      const issues = await this.fetchAllIssues(lastIssueSync?.last_issue_sync);
      console.log(`Fetched ${issues.length} issues`);

      if (issues.length > 0) {
        await this.saveIssues(issues);
        await this.updateIssueSyncState();
        console.log("Issues saved to database");
      }

      // Then sync comments for issues that need it
      let commentsSynced = 0;
      for (const issue of issues) {
        if (issue.comments > 0) {
          console.log(
            `Checking comments for issue #${issue.number} (${issue.comments} comments)`
          );
          const lastCommentSync = await this.getCommentSyncState(issue.id);

          const needsCommentSync =
            !lastCommentSync ||
            new Date(issue.updated_at) > new Date(lastCommentSync);

          if (needsCommentSync) {
            console.log(`Syncing comments for issue #${issue.number}`);
            const comments = await this.fetchCommentsForIssue(
              issue.number,
              lastCommentSync || undefined
            );

            if (comments.length > 0) {
              await this.saveComments(comments, issue.id);
              await this.updateCommentSyncState(issue.id);
              commentsSynced += comments.length;
            }
          }
        }
      }

      console.log(`Sync cycle complete. Synced ${commentsSynced} comments`);
    } catch (error) {
      console.error("Sync cycle failed:", error);
    }
  }

  private async saveIssues(issues: Issue[]): Promise<void> {
    try {
      const db = await getDB();
      await transaction(db, async (conn) => {
        for (const issue of issues) {
          await conn.run(
            `
            INSERT OR REPLACE INTO github_issues (
              id, number, is_pull_request, title, body, state, created_at, updated_at, closed_at,
              user_login, labels, comment_count, repository
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `,
            issue.id,
            issue.number,
            !!issue.pull_request,
            issue.title,
            issue.body,
            issue.state,
            issue.created_at,
            issue.updated_at,
            issue.closed_at,
            issue.user.login,
            JSON.stringify(issue.labels.map((l) => l.name)),
            issue.comments,
            `${this.owner}/${this.repo}`
          );
        }
      });
      console.log(`Saved ${issues.length} issues to database`);
    } catch (error) {
      console.error("Failed to save issues:", error);
      throw error;
    }
  }

  private async saveComments(
    comments: IssueComment[],
    issueId: bigint
  ): Promise<void> {
    try {
      const db = await getDB();
      await transaction(db, async (conn) => {
        // If this is a full sync, clear existing comments
        const lastSync = await this.getCommentSyncState(issueId);
        if (!lastSync) {
          console.log(
            `Performing full comment sync for issue ID ${issueId}, clearing existing comments...`
          );
          await conn.run(
            `DELETE FROM github_comments WHERE issue_id = ? AND repository = ?`,
            issueId,
            `${this.owner}/${this.repo}`
          );
        }

        // Insert or update new comments
        for (const comment of comments) {
          await conn.run(
            `
            INSERT OR REPLACE INTO github_comments (
              id, issue_id, body, user_login, created_at, updated_at, repository
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
          `,
            comment.id,
            issueId,
            comment.body,
            comment.user.login,
            comment.created_at,
            comment.updated_at,
            `${this.owner}/${this.repo}`
          );
        }
      });
      console.log(`Saved ${comments.length} comments for issue ID ${issueId}`);
    } catch (error) {
      console.error(`Failed to save comments for issue ${issueId}:`, error);
      throw error;
    }
  }

  public async forceResyncComments(issueNumber: number): Promise<void> {
    try {
      const db = await getDB();
      console.log(`Force resyncing comments for issue #${issueNumber}...`);
      const issue = await db.all(
        `SELECT id FROM github_issues WHERE number = ? AND repository = ?`,
        issueNumber,
        `${this.owner}/${this.repo}`
      );

      if (issue.length > 0) {
        await db.all(
          `DELETE FROM comment_sync_state WHERE issue_id = ?`,
          issue[0].id
        );

        const comments = await this.fetchCommentsForIssue(issueNumber);
        await this.saveComments(comments, issue[0].id);
        await this.updateCommentSyncState(issue[0].id);
        console.log(`Force resync complete for issue #${issueNumber}`);
      } else {
        console.log(`Issue #${issueNumber} not found`);
      }
    } catch (error) {
      console.error(
        `Failed to force resync comments for issue #${issueNumber}:`,
        error
      );
      throw error;
    }
  }

  public async getIssueComments(issueNumber: number): Promise<any[]> {
    try {
      const db = await getDB();
      return await db.all(
        `
        SELECT c.* 
        FROM github_comments c
        JOIN github_issues i ON i.id = c.issue_id
        WHERE i.number = ? AND i.repository = ?
        ORDER BY c.created_at ASC
      `,
        issueNumber,
        `${this.owner}/${this.repo}`
      );
    } catch (error) {
      console.error(`Failed to get comments for issue #${issueNumber}:`, error);
      throw error;
    }
  }

  public async getSyncStatus(): Promise<any> {
    try {
      const db = await getDB();
      const issueSync = await this.getLastIssueSyncState();
      const repoStats = await db.all(
        `
        SELECT 
          COUNT(DISTINCT i.id) as total_issues,
          COUNT(DISTINCT c.id) as total_comments,
          MAX(i.updated_at) as latest_issue_update,
          MAX(c.updated_at) as latest_comment_update
        FROM github_issues i
        LEFT JOIN github_comments c ON i.id = c.issue_id
        WHERE i.repository = ?
      `,
        `${this.owner}/${this.repo}`
      );

      return {
        repository: `${this.owner}/${this.repo}`,
        last_issue_sync: issueSync?.last_issue_sync || null,
        stats: repoStats[0],
      };
    } catch (error) {
      console.error("Failed to get sync status:", error);
      throw error;
    }
  }

  public async searchIssues(query: string): Promise<any[]> {
    try {
      const db = await getDB();
      return await db.all(
        `
        SELECT i.* 
        FROM github_issues i
        WHERE i.repository = ?
        AND (
          i.title LIKE ?
          OR i.body LIKE ?
          OR i.user_login LIKE ?
          OR i.labels LIKE ?
        )
        ORDER BY i.updated_at DESC
      `,
        `${this.owner}/${this.repo}`,
        `%${query}%`,
        `%${query}%`,
        `%${query}%`,
        `%${query}%`
      );
    } catch (error) {
      console.error(`Failed to search issues with query "${query}":`, error);
      throw error;
    }
  }

  public async searchComments(query: string): Promise<any[]> {
    try {
      const db = await getDB();
      return await db.all(
        `
        SELECT c.*, i.number as issue_number
        FROM github_comments c
        JOIN github_issues i ON i.id = c.issue_id
        WHERE c.repository = ?
        AND (
          c.body LIKE ?
          OR c.user_login LIKE ?
        )
        ORDER BY c.updated_at DESC
      `,
        `${this.owner}/${this.repo}`,
        `%${query}%`,
        `%${query}%`
      );
    } catch (error) {
      console.error(`Failed to search comments with query "${query}":`, error);
      throw error;
    }
  }

  public stop(): void {
    if (this.syncInterval) {
      console.log("Stopping sync process...");
      clearInterval(this.syncInterval);
      this.syncInterval = null;
      console.log("Sync process stopped");
    }
  }

  public async cleanup(): Promise<void> {
    try {
      const db = await getDB();
      console.log("Starting cleanup process...");
      await transaction(db, async (conn) => {
        // Remove all data for this repository
        await conn.run(
          `DELETE FROM github_comments WHERE repository = ?`,
          `${this.owner}/${this.repo}`
        );
        await conn.run(
          `DELETE FROM github_issues WHERE repository = ?`,
          `${this.owner}/${this.repo}`
        );
        await conn.run(
          `DELETE FROM issue_sync_state WHERE repository = ?`,
          `${this.owner}/${this.repo}`
        );
        await conn.run(
          `DELETE FROM comment_sync_state WHERE issue_id IN (
            SELECT id FROM github_issues WHERE repository = ?
          )`,
          `${this.owner}/${this.repo}`
        );
      });
      console.log("Cleanup complete");
    } catch (error) {
      console.error("Failed to cleanup repository data:", error);
      throw error;
    }
  }
}
