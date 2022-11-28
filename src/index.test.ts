import { PostgresDatasourceConfiguration } from '@superblocksteam/shared';

import {
  DUMMY_ACTION_CONFIGURATION,
  DUMMY_DB_DATASOURCE_CONFIGURATION,
  DUMMY_EXECUTION_CONTEXT,
  DUMMY_EXPECTED_METADATA,
  DUMMY_EXTRA_PLUGIN_EXECUTION_PROPS,
  DUMMY_QUERY_RESULT,
  DUMMY_TABLE_RESULT
} from '@superblocksteam/shared-backend';

jest.mock('@superblocksteam/shared-backend', () => {
  const originalModule = jest.requireActual('@superblocksteam/shared-backend');
  return {
    __esModule: true,
    ...originalModule,
    CreateConnection: jest.fn((target, name, descriptor) => {
      return descriptor;
    }),
    DestroyConnection: jest.fn((target, name, descriptor) => {
      return descriptor;
    })
  };
});

import { Client } from 'pg';
jest.mock('pg');

import { KeysQuery, TableQuery } from './queries';
import PostgresPlugin from '.';

const plugin: PostgresPlugin = new PostgresPlugin();
plugin.logger = { debug: (): void => undefined };

const DUMMY_POSTGRES_QUERY_RESULT = {
  rows: DUMMY_QUERY_RESULT
};
const DUMMY_POSTGRES_TABLE_RESULT = {
  rows: DUMMY_TABLE_RESULT
};
const DUMMY_POSTGRES_KEY_RESULT = {
  rows: [
    {
      constraint_name: 'orders_pkey',
      constraint_type: 'p',
      self_schema: 'public',
      self_table: 'orders',
      self_columns: '{id}',
      foreign_schema: null,
      foreign_table: null,
      foreign_columns: '{NULL}',
      definition: 'PRIMARY KEY (id)'
    }
  ]
};
const DUMMY_POSTGRES_EXPECTED_METADATA = {
  ...DUMMY_EXPECTED_METADATA,
  keys: [{ name: 'orders_pkey', type: 'primary_key' }]
};

const context = DUMMY_EXECUTION_CONTEXT;
const datasourceConfiguration = DUMMY_DB_DATASOURCE_CONFIGURATION as PostgresDatasourceConfiguration;
const actionConfiguration = DUMMY_ACTION_CONFIGURATION;
const props = {
  context,
  datasourceConfiguration,
  actionConfiguration,
  ...DUMMY_EXTRA_PLUGIN_EXECUTION_PROPS
};

afterEach(() => {
  jest.restoreAllMocks();
});

describe('Postgres Plugin', () => {
  it('test connection', async () => {
    jest.spyOn(Client.prototype, 'connect').mockImplementation((): void => undefined);
    jest.spyOn(Client.prototype, 'query').mockImplementation((): void => undefined);

    await plugin.test(datasourceConfiguration);

    expect(Client.prototype.connect).toBeCalledTimes(1);
    expect(Client.prototype.connect).toBeCalledTimes(1);
  });

  it('get metadata', async () => {
    jest.spyOn(Client.prototype, 'connect').mockImplementation((): void => undefined);
    jest.spyOn(Client.prototype, 'query').mockImplementation((query) => {
      if (query === KeysQuery) {
        return DUMMY_POSTGRES_KEY_RESULT;
      } else if (query === TableQuery) {
        return DUMMY_POSTGRES_TABLE_RESULT;
      } else {
        return {};
      }
    });

    const res = await plugin.metadata(datasourceConfiguration);

    expect(res.dbSchema?.tables[0]).toEqual(DUMMY_POSTGRES_EXPECTED_METADATA);
  });

  it('execute query', async () => {
    const client = new Client({});
    jest.spyOn(Client.prototype, 'connect').mockImplementation((): void => undefined);
    jest.spyOn(Client.prototype, 'query').mockImplementation((query) => {
      if (query === actionConfiguration.body) {
        return DUMMY_POSTGRES_QUERY_RESULT;
      } else {
        return {};
      }
    });

    const res = await plugin.executePooled(props, client);

    expect(res.output).toEqual(DUMMY_POSTGRES_QUERY_RESULT.rows);
    expect(client.query).toBeCalledTimes(1);
  });
});
