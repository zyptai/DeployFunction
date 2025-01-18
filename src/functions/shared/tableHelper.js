// Copyright (c) 2024 ZyptAI, tim.barrow@zyptai.com
// Proprietary and confidential to ZyptAI
// File: src/functions/shared/tableHelper.js
// Purpose: Helper functions for Azure Table storage operations in deployment tracking

const { TableClient, TableServiceClient } = require("@azure/data-tables");

class TableHelper {
    constructor(connectionString) {
        this.connectionString = connectionString;
        this.tableService = TableServiceClient.fromConnectionString(connectionString);
        this.tables = {
            customers: "Customers",
            environments: "CustomerEnvironments",
            deployments: "DeploymentHistory",
            resources: "DeployedResources",
            endpoints: "IntegrationEndpoints"
        };
    }

    async getTableClient(tableName) {
        return TableClient.fromConnectionString(this.connectionString, tableName);
    }

    // Customer operations
    async createCustomer(customerData) {
        try {
            const client = await this.getTableClient(this.tables.customers);
            const entity = {
                partitionKey: customerData.customerId,
                rowKey: customerData.customerId,
                timestamp: new Date().toISOString(),
                ...customerData
            };
            await client.createEntity(entity);
            return true;
        } catch (error) {
            throw new Error(`Error creating customer record: ${error.message}`);
        }
    }

    // Environment operations
    async createEnvironment(environmentData) {
        try {
            const client = await this.getTableClient(this.tables.environments);
            const entity = {
                partitionKey: environmentData.customerId,
                rowKey: environmentData.environmentId,
                timestamp: new Date().toISOString(),
                ...environmentData
            };
            await client.createEntity(entity);
            return true;
        } catch (error) {
            throw new Error(`Error creating environment record: ${error.message}`);
        }
    }

    // Deployment operations
    async createDeploymentRecord(deploymentData) {
        try {
            const client = await this.getTableClient(this.tables.deployments);
            const entity = {
                partitionKey: deploymentData.customerId,
                rowKey: `${Date.now()}_${deploymentData.environmentId}`,
                timestamp: new Date().toISOString(),
                status: 'InProgress',
                ...deploymentData
            };
            await client.createEntity(entity);
            return entity.rowKey;
        } catch (error) {
            throw new Error(`Error creating deployment record: ${error.message}`);
        }
    }

    // Resource operations
    async createResourceRecord(resourceData) {
        try {
            const client = await this.getTableClient(this.tables.resources);
            const entity = {
                partitionKey: resourceData.deploymentId,
                rowKey: `${resourceData.resourceType}_${Date.now()}`,
                timestamp: new Date().toISOString(),
                status: 'Deployed',
                ...resourceData
            };
            await client.createEntity(entity);
            return true;
        } catch (error) {
            throw new Error(`Error creating resource record: ${error.message}`);
        }
    }

    // Integration endpoint operations
    async createEndpoint(endpointData) {
        try {
            const client = await this.getTableClient(this.tables.endpoints);
            const entity = {
                partitionKey: endpointData.customerId,
                rowKey: `${endpointData.serviceType}_${Date.now()}`,
                timestamp: new Date().toISOString(),
                status: 'Active',
                ...endpointData
            };
            await client.createEntity(entity);
            return true;
        } catch (error) {
            throw new Error(`Error creating endpoint record: ${error.message}`);
        }
    }

    // Query operations
    async queryEntities(tableName, customerId) {
        try {
            const client = await this.getTableClient(tableName);
            const entities = client.listEntities({
                queryOptions: { filter: `PartitionKey eq '${customerId}'` }
            });
            
            const results = [];
            for await (const entity of entities) {
                results.push(entity);
            }
            return results;
        } catch (error) {
            throw new Error(`Error querying entities: ${error.message}`);
        }
    }

    // Update operations
    async updateEntity(tableName, entity) {
        try {
            const client = await this.getTableClient(tableName);
            await client.updateEntity(entity, "Merge");
            return true;
        } catch (error) {
            throw new Error(`Error updating entity: ${error.message}`);
        }
    }
}

module.exports = TableHelper;