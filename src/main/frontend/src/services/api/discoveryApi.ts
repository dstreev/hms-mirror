import BaseApi from './baseApi';

export interface DatabaseListResponse {
  status: string;
  databases: string[];
  count: number;
  message?: string;
}

export interface TableListResponse {
  status: string;
  tables: string[];
  count: number;
  databaseName: string;
  message?: string;
}

/**
 * API for discovering databases and tables from tested connections.
 * Provides methods to query metadata from HiveServer2 connections.
 */
class DiscoveryApi extends BaseApi {
  constructor() {
    super('/hms-mirror/api/v1');
  }

  /**
   * Fetch list of databases from a connection.
   *
   * @param connectionKey The connection key to query
   * @returns Promise with database list or null on error
   */
  async getDatabases(connectionKey: string): Promise<string[] | null> {
    try {
      const response = await this.get<DatabaseListResponse>(
        `/connections/${connectionKey}/databases`
      );

      if (response?.status === 'success' && response.databases) {
        return response.databases;
      }

      console.error('Failed to fetch databases:', response?.message);
      return null;
    } catch (error: any) {
      console.error(`Failed to fetch databases for connection ${connectionKey}:`, error);
      console.error('Error details:', error.response?.data || error.message);
      return null;
    }
  }

  /**
   * Fetch list of tables from a specific database in a connection.
   *
   * @param connectionKey The connection key to query
   * @param databaseName The database name to query tables from
   * @returns Promise with table list or null on error
   */
  async getTables(connectionKey: string, databaseName: string): Promise<string[] | null> {
    try {
      const response = await this.get<TableListResponse>(
        `/connections/${connectionKey}/databases/${databaseName}/tables`
      );

      if (response?.status === 'success' && response.tables) {
        return response.tables;
      }

      console.error('Failed to fetch tables:', response?.message);
      return null;
    } catch (error: any) {
      console.error(
        `Failed to fetch tables for database ${databaseName} in connection ${connectionKey}:`,
        error
      );
      console.error('Error details:', error.response?.data || error.message);
      return null;
    }
  }
}

export const discoveryApi = new DiscoveryApi();
export default DiscoveryApi;
