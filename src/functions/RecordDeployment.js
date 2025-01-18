// Copyright (c) 2024 ZyptAI, tim.barrow@zyptai.com
// Proprietary and confidential to ZyptAI
// File: src/functions/RecordDeployment.js
// Purpose: Azure Function to record customer deployment information in tracking tables

const { app } = require('@azure/functions');
const TableHelper = require('./shared/tableHelper');

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

            // Initialize table helper
            const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
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
            results.deployment = await tableHelper.createDeploymentRecord({
                customerId: body.customerId,
                environmentId: body.environmentId,
                deploymentType: body.deploymentType || 'Initial',
                ...body.deploymentDetails
            });

            // Create or update customer record if provided
            if (body.customerDetails) {
                results.customer = await tableHelper.createCustomer({
                    customerId: body.customerId,
                    ...body.customerDetails
                });
            }

            // Create environment record if provided
            if (body.environmentDetails) {
                results.environment = await tableHelper.createEnvironment({
                    customerId: body.customerId,
                    environmentId: body.environmentId,
                    ...body.environmentDetails
                });
            }

            // Create resource records if provided
            if (body.resources && Array.isArray(body.resources)) {
                for (const resource of body.resources) {
                    await tableHelper.createResourceRecord({
                        deploymentId: results.deployment,
                        customerId: body.customerId,
                        ...resource
                    });
                    results.resources.push(resource.resourceType);
                }
            }

            // Create endpoint records if provided
            if (body.endpoints && Array.isArray(body.endpoints)) {
                for (const endpoint of body.endpoints) {
                    await tableHelper.createEndpoint({
                        customerId: body.customerId,
                        ...endpoint
                    });
                    results.endpoints.push(endpoint.serviceType);
                }
            }
            
            const endTime = new Date().getTime();
            context.log(`Total execution time: ${endTime - startTime}ms`);

            return {
                status: 200,
                body: JSON.stringify({
                    success: true,
                    deploymentId: results.deployment,
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
                    stack: error.stack
                })
            };
        }
    }
});