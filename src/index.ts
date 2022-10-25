import {
  Column,
  DatasourceMetadataDto,
  ExecutionOutput,
  IntegrationError,
  PostgresActionConfiguration,
  PostgresDatasourceConfiguration,
  RawRequest,
  Table,
  TableType
} from '@superblocksteam/shared';
import {
  normalizeTableColumnNames,
  PluginExecutionProps,
  DatabasePlugin,
  CreateConnection,
  DestroyConnection
} from '@superblocksteam/shared-backend';
import { isEmpty } from 'lodash';
import { Client, Notification } from 'pg';
import { NoticeMessage } from 'pg-protocol/dist/messages';
import { KeysQuery, TableQuery } from './queries';

const TEST_CONNECTION_TIMEOUT = 5000;

export default class PostgresPlugin extends DatabasePlugin {
  constructor() {
    super({ useOrderedParameters: true });
  }

  public async execute({
    context,
    datasourceConfiguration,
    actionConfiguration
  }: PluginExecutionProps<PostgresDatasourceConfiguration>): Promise<ExecutionOutput> {
    const client = await this.createConnection(datasourceConfiguration);
    const query = actionConfiguration.body;
    const ret = new ExecutionOutput();
    if (isEmpty(query)) {
      return ret;
    }
    let rows;
    try {
      rows = await this.executeQuery(() => {
        return client.query(query, context.preparedStatementContext);
      });
    } catch (err) {
      throw new IntegrationError(`Postgres query failed, ${err.message}`);
    } finally {
      if (client) {
        this.destroyConnection(client).catch(() => {
          // Error handling is done in the decorator
        });
      }
    }
    ret.output = normalizeTableColumnNames(rows.rows);
    return ret;
  }

  public getRequest(actionConfiguration: PostgresActionConfiguration): RawRequest {
    return actionConfiguration?.body;
  }

  public dynamicProperties(): string[] {
    return ['body'];
  }

  public async metadata(datasourceConfiguration: PostgresDatasourceConfiguration): Promise<DatasourceMetadataDto> {
    const client = await this.createConnection(datasourceConfiguration);
    try {
      // table
      const tableResult = await this.executeQuery(async () => {
        return client.query(TableQuery);
      });
      const entities = tableResult.rows.reduce((acc, attribute) => {
        const entityName = attribute['table_name'];
        const entityType = TableType.TABLE;

        const entity = acc.find((o) => o.name === entityName);
        if (entity) {
          const columns = entity.columns;
          const isColumnNameCased = !(attribute.name === attribute.name.toLowerCase());
          entity.columns = [...columns, new Column(isColumnNameCased ? `"${attribute.name}"` : attribute.name, attribute.column_type)];
          return [...acc];
        }

        const table = new Table(entityName, entityType);
        table.columns.push(new Column(attribute.name, attribute.column_type));

        return [...acc, table];
      }, []);

      // keys
      const keysResult = await this.executeQuery(async () => {
        return client.query(KeysQuery);
      });
      keysResult.rows.forEach((key) => {
        const table = entities.find((e) => e.name === key.self_table);
        if (table) {
          table.keys.push({ name: key.constraint_name, type: key.constraint_type === 'p' ? 'primary_key' : 'foreign_key' });
        }
      });
      return {
        dbSchema: { tables: entities }
      };
    } catch (err) {
      throw new IntegrationError(`Failed to connect to Postgres, ${err.message}`);
    } finally {
      if (client) {
        this.destroyConnection(client).catch(() => {
          // Error handling is done in the decorator
        });
      }
    }
  }

  @CreateConnection
  private async createConnection(
    datasourceConfiguration: PostgresDatasourceConfiguration,
    connectionTimeoutMillis = 30000
  ): Promise<Client> {
    if (!datasourceConfiguration) {
      throw new IntegrationError('Datasource not found for Postgres step');
    }
    const endpoint = datasourceConfiguration.endpoint;
    const auth = datasourceConfiguration.authentication;
    if (!endpoint) {
      throw new IntegrationError('Endpoint not specified for Postgres step');
    }
    if (!auth) {
      throw new IntegrationError('Authentication not specified for Postgres step');
    }
    if (!auth.custom?.databaseName?.value) {
      throw new IntegrationError('Database not specified for Postgres step');
    }

    let ssl_config: Record<string, unknown> | undefined;
    if (datasourceConfiguration.connection?.useSsl) {
      ssl_config = {
        rejectUnauthorized: false
      };
      if (datasourceConfiguration.connection?.useSelfSignedSsl) {
        ssl_config.ca = datasourceConfiguration.connection?.ca;
        ssl_config.key = datasourceConfiguration.connection?.key;
        ssl_config.cert = datasourceConfiguration.connection?.cert;
      }
    }

    const client = new Client({
      host: endpoint.host,
      user: auth.username,
      password: auth.password,
      database: auth.custom.databaseName.value,
      port: endpoint.port,
      ssl: ssl_config,
      connectionTimeoutMillis: connectionTimeoutMillis
    });
    this.attachLoggerToClient(client, datasourceConfiguration);

    await client.connect();
    this.logger.debug(`Postgres client connected. ${datasourceConfiguration.endpoint?.host}:${datasourceConfiguration.endpoint?.port}`);
    return client;
  }

  @DestroyConnection
  private async destroyConnection(client: Client): Promise<void> {
    await client.end();
  }

  private attachLoggerToClient(client: Client, datasourceConfiguration: PostgresDatasourceConfiguration) {
    if (!datasourceConfiguration.endpoint) {
      return;
    }

    const datasourceEndpoint = `${datasourceConfiguration.endpoint?.host}:${datasourceConfiguration.endpoint?.port}`;

    client.on('error', (err: Error) => {
      this.logger.error(`Postgres client error. ${datasourceEndpoint} ${err.stack}`);
    });

    client.on('end', () => {
      this.logger.debug(`Postgres client disconnected from server. ${datasourceEndpoint}`);
    });

    client.on('notification', (message: Notification): void => {
      this.logger.debug(`Postgres notification ${message}. ${datasourceEndpoint}`);
    });

    client.on('notice', (notice: NoticeMessage) => {
      this.logger.debug(`Postgres notice: ${notice.message}. ${datasourceEndpoint}`);
    });
  }

  public async test(datasourceConfiguration: PostgresDatasourceConfiguration): Promise<void> {
    let client: Client | null = null;
    try {
      client = await this.createConnection(datasourceConfiguration, TEST_CONNECTION_TIMEOUT);
      await this.executeQuery(() => {
        return client.query('SELECT NOW()');
      });
    } catch (err) {
      throw new IntegrationError(`Test Postgres connection failed, ${err.message}`);
    } finally {
      if (client) {
        this.destroyConnection(client).catch(() => {
          // Error handling is done in the decorator
        });
      }
    }
  }
}
