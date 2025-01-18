// Copyright (c) 2024 ZyptAI, tim.barrow@zyptai.com
// Proprietary and confidential to ZyptAI
// File: src/functions/RecordDeployment.js
// Purpose: Azure Function to record customer deployment information in tracking tables

const { app } = require('@azure/functions');
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
}

app.http('RecordDeployment', {
    methods: ['POST'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        try {
            const startTime = new Date().getTime();
            context.log('Starting deployment record processing');
            
            // Parse and validate request
            const body = await request.json();
            context.log(`Request body: ${JSON.stringify(body)}`);

            if (!body || !body.customerId || !body.environmentId) {
                context.log.error('Missing required fields in request body');
                return {
                    status: 400,
                    body: JSON.stringify({
                        error: "Please provide customerId and environmentId in the request body"
                    })
                };
            }

            // Log connection string existence (not the value)
            const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
            context.log(`Connection string exists: ${!!connectionString}`);

            try {
                // Initialize table helper
                context.log('Initializing TableHelper');
                const tableHelper = new TableHelper(connectionString);
                
                // Create records in each table
                const results = {
                    deployment: null,
                    customer: false,
                    environment: false,
                    resources: [],
                    endpoints: []
                };

                // Create deployment record
                context.log('Creating deployment record');
                results.deployment = await tableHelper.createDeploymentRecord({
                    customerId: body.customerId,
                    environmentId: body.environmentId,
                    deploymentType: body.deploymentType || 'Initial',
                    ...body.deploymentDetails
                });
                context.log(`Deployment record created: ${results.deployment}`);

                // Create or update customer record if provided
                if (body.customerDetails) {
                    context.log('Creating customer record');
                    results.customer = await tableHelper.createCustomer({
                        customerId: body.customerId,
                        ...body.customerDetails
                    });
                    context.log('Customer record created');
                }

                // Create environment record if provided
                if (body.environmentDetails) {
                    context.log('Creating environment record');
                    results.environment = await tableHelper.createEnvironment({
                        customerId: body.customerId,
                        environmentId: body.environmentId,
                        ...body.environmentDetails
                    });
                    context.log('Environment record created');
                }

                // Create resource records if provided
                if (body.resources && Array.isArray(body.resources)) {
                    context.log('Creating resource records');
                    for (const resource of body.resources) {
                        await tableHelper.createResourceRecord({
                            deploymentId: results.deployment,
                            customerId: body.customerId,
                            ...resource
                        });
                        results.resources.push(resource.resourceType);
                    }
                    context.log(`Resource records created: ${results.resources.length}`);
                }

                // Create endpoint records if provided
                if (body.endpoints && Array.isArray(body.endpoints)) {
                    context.log('Creating endpoint records');
                    for (const endpoint of body.endpoints) {
                        await tableHelper.createEndpoint({
                            customerId: body.customerId,
                            ...endpoint
                        });
                        results.endpoints.push(endpoint.serviceType);
                    }
                    context.log(`Endpoint records created: ${results.endpoints.length}`);
                }

            } catch (innerError) {
                context.log.error(`Error in table operations: ${innerError.message}`);
                context.log.error(`Inner error stack: ${innerError.stack}`);
                throw innerError;
            }
            
            const endTime = new Date().getTime();
            context.log(`Total execution time: ${endTime - startTime}ms`);

            return {
                status: 200,
                body: JSON.stringify({
                    success: true,
                    message: "Deployment record created",
                    executionTime: endTime - startTime
                })
            };

        } catch (error) {
            context.log.error(`Error in RecordDeployment: ${error.message}`);
            context.log.error('Stack:', error.stack);
            
            return {
                status: 500,
                body: JSON.stringify({
                    error: "Operation failed",
                    message: error.message,
                    stack: error.stack
                })
            };
        }
    }
});