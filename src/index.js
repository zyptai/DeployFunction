// Copyright (c) 2024 ZyptAI, tim.barrow@zyptai.com
// Proprietary and confidential to ZyptAI
// File: src/index.js
// Purpose: Entry point for Azure Functions app, configures HTTP streaming support for deployment tracking

const { app } = require('@azure/functions');

app.setup({
    enableHttpStream: true,
});