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

    async testTableAccess(context, tableName) {
        try {
            context.log(`Testing access to table: ${tableName}`);
            const client = TableClient.fromConnectionString(this.connectionString, tableName);
            
            // Try to query top 1 to test access
            const iterator = client.listEntities({
                queryOptions: { top: 1 }
            });
            
            await iterator.next();
            context.log(`Successfully accessed table: ${tableName}`);
            return true;
        } catch (error) {
            context.log.error(`Table access test failed for ${tableName}`);
            context.log.error(`Error type: ${error.constructor.name}`);
            context.log.error(`Error code: ${error.statusCode}`);
            context.log.error(`Error message: ${error.message}`);
            return false;
        }
    }

    async ensureTableExists(tableName) {
        try {
            const tableClient = TableClient.fromConnectionString(this.connectionString, tableName);
            await tableClient.createTable();
            return true;
        } catch (error) {
            if (error.statusCode === 409) {
                // Table already exists, which is fine
                return true;
            }
            throw error;
        }
    }

    async createCustomer(customerData, context) {
        try {
            await this.ensureTableExists(this.tables.customers);
            const hasAccess = await this.testTableAccess(context, this.tables.customers);
            if (!hasAccess) {
                throw new Error(`No access to table ${this.tables.customers}`);
            }

            const client = TableClient.fromConnectionString(this.connectionString, this.tables.customers);
            context.log(`Creating customer entity with ID: ${customerData.customerId}`);
            
            const entity = {
                partitionKey: customerData.customerId,
                rowKey: customerData.customerId,
                timestamp: new Date().toISOString(),
                ...customerData
            };
            
            context.log('Attempting to create customer entity');
            await client.createEntity(entity);
            context.log('Customer entity created successfully');
            return true;
        } catch (error) {
            context.log.error(`Failed to create customer entity: ${error.message}`);
            context.log.error(`Error type: ${error.constructor.name}`);
            context.log.error(`Error details: ${JSON.stringify({
                code: error.statusCode,
                message: error.message,
                details: error.details || 'No additional details'
            })}`);
            throw error;
        }
    }

    async createEnvironment(environmentData, context) {
        try {
            await this.ensureTableExists(this.tables.environments);
            const hasAccess = await this.testTableAccess(context, this.tables.environments);
            if (!hasAccess) {
                throw new Error(`No access to table ${this.tables.environments}`);
            }

            const client = TableClient.fromConnectionString(this.connectionString, this.tables.environments);
            const entity = {
                partitionKey: environmentData.customerId,
                rowKey: environmentData.environmentId,
                timestamp: new Date().toISOString(),
                ...environmentData
            };
            await client.createEntity(entity);
            return true;
        } catch (error) {
            context.log.error(`Failed to create environment: ${error.message}`);
            throw error;
        }
    }

    async createDeploymentRecord(deploymentData, context) {
        try {
            await this.ensureTableExists(this.tables.deployments);
            const hasAccess = await this.testTableAccess(context, this.tables.deployments);
            if (!hasAccess) {
                throw new Error(`No access to table ${this.tables.deployments}`);
            }

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
            context.log.error(`Failed to create deployment record: ${error.message}`);
            throw error;
        }
    }

    async createResourceRecord(resourceData, context) {
        try {
            await this.ensureTableExists(this.tables.resources);
            const hasAccess = await this.testTableAccess(context, this.tables.resources);
            if (!hasAccess) {
                throw new Error(`No access to table ${this.tables.resources}`);
            }

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
            context.log.error(`Failed to create resource record: ${error.message}`);
            throw error;
        }
    }

    async createEndpoint(endpointData, context) {
        try {
            await this.ensureTableExists(this.tables.endpoints);
            const hasAccess = await this.testTableAccess(context, this.tables.endpoints);
            if (!hasAccess) {
                throw new Error(`No access to table ${this.tables.endpoints}`);
            }

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
            context.log.error(`Failed to create endpoint record: ${error.message}`);
            throw error;
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
            
            // Log identity information
            context.log('Function identity context:', {
                principalId: context.bindingData?.identityPrincipalId,
                tenantId: context.bindingData?.identityTenantId,
                clientId: context.bindingData?.identityClientId
            });

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

            // Test connection string
            context.log('Testing storage connection string format');
            const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
            if (!connectionString) {
                throw new Error('Storage connection string is missing');
            }
            context.log('Connection string format verification:', {
                hasAccountName: connectionString.includes('AccountName='),
                hasAccountKey: connectionString.includes('AccountKey='),
                hasEndpoint: connectionString.includes('DefaultEndpointsProtocol=')
            });

            // Initialize helper and test all tables
            const tableHelper = new TableHelper(connectionString);
            for (const [key, tableName] of Object.entries(tableHelper.tables)) {
                await tableHelper.testTableAccess(context, tableName);
            }

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
            }, context);
            context.log(`Deployment record created: ${results.deployment}`);

            // Create or update customer record if provided
            if (body.customerDetails) {
                context.log('Creating customer record');
                results.customer = await tableHelper.createCustomer({
                    customerId: body.customerId,
                    ...body.customerDetails
                }, context);
                context.log('Customer record created');
            }

            // Create environment record if provided
            if (body.environmentDetails) {
                context.log('Creating environment record');
                results.environment = await tableHelper.createEnvironment({
                    customerId: body.customerId,
                    environmentId: body.environmentId,
                    ...body.environmentDetails
                }, context);
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
                    }, context);
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
                    }, context);
                    results.endpoints.push(endpoint.serviceType);
                }
                context.log(`Endpoint records created: ${results.endpoints.length}`);
            }
            
            const endTime = new Date().getTime();
            context.log(`Total execution time: ${endTime - startTime}ms`);

            return {
                status: 200,
                body: JSON.stringify({
                    success: true,
                    results: results,
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
                    stack: error.stack,
                    type: error.constructor.name
                })
            };
        }
    }
});