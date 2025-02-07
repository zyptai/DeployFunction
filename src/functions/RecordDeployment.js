// Copyright (c) 2024 ZyptAI, tim.barrow@zyptai.com
// Proprietary and confidential to ZyptAI
// File: src/functions/RecordDeployment.js
// Purpose: Azure Function to record customer deployment information

const { app } = require('@azure/functions');
const { TableClient } = require("@azure/data-tables");
const TableHelper = require('./shared/tableHelper');

// Add retry logic helper
const retryOperation = async (operation, maxRetries = 3, delay = 1000) => {
    for (let i = 0; i < maxRetries; i++) {
        try {
            return await operation();
        } catch (error) {
            if (i === maxRetries - 1) throw error; // Last attempt, throw the error
            if (error.name === 'SocketException' || error.name === 'TimeoutError') {
                await new Promise(resolve => setTimeout(resolve, delay * Math.pow(2, i))); // Exponential backoff
                continue;
            }
            throw error; // For other errors, throw immediately
        }
    }
};

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

            // Initialize helper with retries
            const tableHelper = new TableHelper(process.env.AZURE_STORAGE_CONNECTION_STRING);
            const results = {
                deployment: null,
                customer: false,
                environment: false,
                resources: [],
                endpoints: []
            };

            // Create deployment record with retry
            context.log('Creating deployment record');
            results.deployment = await retryOperation(async () => {
                return await tableHelper.createDeploymentRecord({
                    customerId: body.customerId,
                    environmentId: body.environmentId,
                    deploymentType: body.deploymentType || 'Initial',
                    ...body.deploymentDetails
                });
            });
            context.log(`Deployment record created: ${results.deployment}`);

            // Create or update customer record if provided
            if (body.customerDetails) {
                context.log('Creating customer record');
                results.customer = await retryOperation(async () => {
                    return await tableHelper.createCustomer({
                        customerId: body.customerId,
                        ...body.customerDetails
                    });
                });
                context.log('Customer record created');
            }

            // Create environment record with retry
            if (body.environmentDetails) {
                context.log('Creating environment record');
                results.environment = await retryOperation(async () => {
                    return await tableHelper.createEnvironment({
                        customerId: body.customerId,
                        environmentId: body.environmentId,
                        ...body.environmentDetails
                    });
                });
                context.log('Environment record created');
            }

            // Create resource records with retry
            if (body.resources && Array.isArray(body.resources)) {
                context.log('Creating resource records');
                for (const resource of body.resources) {
                    await retryOperation(async () => {
                        await tableHelper.createResourceRecord({
                            deploymentId: results.deployment,
                            customerId: body.customerId,
                            ...resource
                        });
                        results.resources.push(resource.resourceType);
                    });
                }
                context.log(`Resource records created: ${results.resources.length}`);
            }

            // Create endpoint records with retry
            if (body.endpoints && Array.isArray(body.endpoints)) {
                context.log('Creating endpoint records');
                for (const endpoint of body.endpoints) {
                    await retryOperation(async () => {
                        await tableHelper.createEndpoint({
                            customerId: body.customerId,
                            ...endpoint
                        });
                        results.endpoints.push(endpoint.serviceType);
                    });
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
            
            // Enhanced error response
            return {
                status: error.code === 'ETIMEDOUT' ? 408 : 500,
                body: JSON.stringify({
                    error: "Operation failed",
                    message: error.message,
                    type: error.constructor.name,
                    code: error.code,
                    isRetryable: error.name === 'SocketException' || error.name === 'TimeoutError',
                    recommendedAction: error.name === 'SocketException' ? 
                        'Network connectivity issue detected. Please try again.' : 
                        'An unexpected error occurred. Please contact support if the issue persists.'
                })
            };
        }
    }
});