import {
  Column,
  DatasourceMetadataDto,
  ExecutionOutput,
  IntegrationError,
  PostgresActionConfiguration,
  PostgresDatasourceConfiguration,
  RawRequest,
  ResolvedActionConfigurationProperty,
  Table,
  TableType
} from '@superblocksteam/shared';
import {
  ActionConfigurationResolutionContext,
  BasePlugin,
  normalizeTableColumnNames,
  PluginExecutionProps,
  resolveActionConfigurationPropertyUtil
} from '@superblocksteam/shared-backend';
import { isEmpty } from 'lodash';
import { Client, Notification } from 'pg';
import { NoticeMessage } from 'pg-protocol/dist/messages';
import { KeysQuery, TableQuery } from './queries';

const TEST_CONNECTION_TIMEOUT = 5000;

export default class PostgresPlugin extends BasePlugin {
  async resolveActionConfigurationProperty({
    context,
    actionConfiguration,
    files,
    property,
    escapeStrings
  }: // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ActionConfigurationResolutionContext): Promise<ResolvedActionConfigurationProperty> {
    return resolveActionConfigurationPropertyUtil(super.resolveActionConfigurationProperty, {
      context,
      actionConfiguration,
      files,
      property,
      escapeStrings
    });
  }

  async execute({
    context,
    datasourceConfiguration,
    actionConfiguration
  }: PluginExecutionProps<PostgresDatasourceConfiguration>): Promise<ExecutionOutput> {
    const client = await this.createClient(datasourceConfiguration);
    try {
      const query = actionConfiguration.body;
      const ret = new ExecutionOutput();
      if (isEmpty(query)) {
        return ret;
      }
      const rows = await client.query(query, context.preparedStatementContext);
      ret.output = normalizeTableColumnNames(rows.rows);
      return ret;
    } catch (err) {
      throw new IntegrationError(`Postgres query failed, ${err.message}`);
    } finally {
      if (client) {
        await client.end();
      }
    }
  }

  getRequest(actionConfiguration: PostgresActionConfiguration): RawRequest {
    return actionConfiguration?.body;
  }

  dynamicProperties(): string[] {
    return ['body'];
  }

  async metadata(datasourceConfiguration: PostgresDatasourceConfiguration): Promise<DatasourceMetadataDto> {
    let client: Client | undefined;
    try {
      client = await this.createClient(datasourceConfiguration);
      // tables
      const tableResult = await client.query(TableQuery);

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
      const keysResult = await client.query(KeysQuery);
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
        await client.end();
      }
    }
  }

  private async createClient(datasourceConfiguration: PostgresDatasourceConfiguration, connectionTimeoutMillis = 30000): Promise<Client> {
    if (!datasourceConfiguration) {
      throw new IntegrationError('Datasource not found for Postgres step');
    }
    try {
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
    } catch (err) {
      throw new IntegrationError(`Failed to connect to Postgres, ${err.message}`);
    }
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

  async test(datasourceConfiguration: PostgresDatasourceConfiguration): Promise<void> {
    let client: Client | null = null;
    try {
      client = await this.createClient(datasourceConfiguration, TEST_CONNECTION_TIMEOUT);
      await client.query('SELECT NOW()');
    } catch (err) {
      throw new IntegrationError(`Test Postgres connection failed, ${err.message}`);
    } finally {
      if (client) {
        await client.end();
      }
    }
  }
}
