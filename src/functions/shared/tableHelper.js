// Copyright (c) 2024 ZyptAI, tim.barrow@zyptai.com
// Proprietary and confidential to ZyptAI
// File: src/functions/shared/tableHelper.js
// Purpose: Helper functions for Azure Table Storage operations in deployment tracking

const { TableClient } = require("@azure/data-tables");

class TableHelper {
    constructor(connectionString) {
        this.connectionString = connectionString;
        this.tables = {
            customers: "Customers",
            environments: "CustomerEnvironments",
            deployments: "DeploymentHistory",
            resources: "DeployedResources",
            endpoints: "IntegrationEndpoints"
        };
    }

    async createCustomer(customerData) {
        try {
            const client = TableClient.fromConnectionString(this.connectionString, this.tables.customers);
            const entity = {
                partitionKey: customerData.customerId,
                rowKey: customerData.customerId,
                timestamp: new Date().toISOString(),
                ...customerData
            };
            await client.upsertEntity(entity, "Replace");
            return true;
        } catch (error) {
            throw new Error(`Customer operation failed: ${error.message}`);
        }
    }

    async createEnvironment(environmentData) {
        try {
            const client = TableClient.fromConnectionString(this.connectionString, this.tables.environments);
            const entity = {
                partitionKey: environmentData.customerId,
                rowKey: environmentData.environmentId,
                timestamp: new Date().toISOString(),
                ...environmentData
            };
            await client.upsertEntity(entity, "Replace");
            return true;
        } catch (error) {
            throw new Error(`Environment operation failed: ${error.message}`);
        }
    }

    async createDeploymentRecord(deploymentData) {
        try {
            const client = TableClient.fromConnectionString(this.connectionString, this.tables.deployments);
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
            throw new Error(`Deployment operation failed: ${error.message}`);
        }
    }

    async createResourceRecord(resourceData) {
        try {
            const client = TableClient.fromConnectionString(this.connectionString, this.tables.resources);
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
            throw new Error(`Resource operation failed: ${error.message}`);
        }
    }

    async createEndpoint(endpointData) {
        try {
            const client = TableClient.fromConnectionString(this.connectionString, this.tables.endpoints);
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
            throw new Error(`Endpoint operation failed: ${error.message}`);
        }
    }

    async queryEntities(tableName, customerId) {
        try {
            const client = TableClient.fromConnectionString(this.connectionString, tableName);
            const entities = client.listEntities({
                queryOptions: { filter: `PartitionKey eq '${customerId}'` }
            });
            
            const results = [];
            for await (const entity of entities) {
                results.push(entity);
            }
            return results;
        } catch (error) {
            throw new Error(`Query operation failed: ${error.message}`);
        }
    }

    async updateEntity(tableName, entity) {
        try {
            const client = TableClient.fromConnectionString(this.connectionString, tableName);
            await client.updateEntity(entity, "Merge");
            return true;
        } catch (error) {
            throw new Error(`Update operation failed: ${error.message}`);
        }
    }

    async batchOperation(tableName, entities, operation = "create") {
        try {
            const client = TableClient.fromConnectionString(this.connectionString, tableName);
            const batch = [];
            
            for (const entity of entities) {
                batch.push({
                    operation: operation === "create" ? "upsert" : operation,
                    entity: {
                        ...entity,
                        timestamp: new Date().toISOString()
                    }
                });
            }
            
            if (batch.length > 0) {
                await client.submitTransaction(batch);
            }
            return true;
        } catch (error) {
            throw new Error(`Batch operation failed: ${error.message}`);
        }
    }
}

module.exports = TableHelper;