import { EventEmitter } from 'events';
EventEmitter.defaultMaxListeners = 20;

import express from 'express';
import Web3 from 'web3';
import mysql from 'mysql2/promise';
import cron from 'node-cron';
import axios from 'axios';
import dotenv from 'dotenv';
import rateLimit from 'express-rate-limit';
import cors from 'cors';
import moment from 'moment';

dotenv.config();

import contractsLib from './utils/contract_addresses.json' assert {type: 'json'};
import dsfABI from './utils/dsf_abi.json' assert {type: 'json'};
import dsfStrategyABI from './utils/dsf_strategy_abi.json' assert {type: 'json'};
import dsfRatioABI from './utils/dsf_ratio_abi.json' assert {type: 'json'};
import crvRewardsABI from './utils/crv_rewards_abi.json' assert {type: 'json'};
import uniswapRouterABI from './utils/uniswap_router_abi.json' assert {type: 'json'};
import crvABI from './utils/CRV_abi.json' assert {type: 'json'};
import cvxABI from './utils/CVX_abi.json' assert {type: 'json'};
import usdtABI from './utils/USDT_abi.json' assert {type: 'json'};

// Импорт ботов 
import { sendMessageToChat } from './utils/bots/telegramBotEvets.mjs';

const app = express();

// Ограничение частоты запросов
const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 минут
    max: 100, // Ограничение: 100 запросов с одного IP за 15 минут
    message: "Too many requests from this IP, please try again later."
});

app.use(limiter);

// Настройка CORS для всех доменов
const corsOptions = {
    origin: '*', // Разрешить доступ всем источникам
    methods: ['GET', 'POST', 'PUT', 'DELETE'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: true
};

app.use(cors(corsOptions));

//Цвета
const colors = {
    red: '\x1b[31m',
    yellow: '\x1b[33m',
    green: '\x1b[32m',
    blue: '\x1b[34m',
    magenta: '\x1b[35m',
    reset: '\x1b[0m'
};

function logError(message) {
    console.error(`${colors.red}${message}${colors.reset}`);
}

function logWarning(message) {
    console.warn(`${colors.yellow}${message}${colors.reset}`);
}

function logSuccess(message) {
    console.log(`${colors.green}${message}${colors.reset}`);
}

function logInfo(message) {
    console.log(`${colors.blue}${message}${colors.reset}`);
}

function logDebug(message) {
    console.debug(`${colors.magenta}${message}${colors.reset}`);
}

// Middleware to redirect HTTP to HTTPS
app.use((req, res, next) => {
    if (req.headers['x-forwarded-proto'] !== 'https') {
        return res.redirect(`https://${req.headers.host}${req.url}`);
    }
    next();
});

let pool;

// Функция для создания пула соединений
async function createPool() {
    pool = mysql.createPool({
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: process.env.DB_NAME,
        waitForConnections: true,
        connectionLimit: 10,
        queueLimit: 0,
        connectTimeout: 10000 // Увеличение времени ожидания подключения до 10 секунд
    });
}

createPool();

// const pool = mysql.createPool({
//     host: process.env.DB_HOST,
//     user: process.env.DB_USER,
//     password: process.env.DB_PASSWORD,
//     database: process.env.DB_NAME,
//     waitForConnections: true,
//     connectionLimit: 10,
//     queueLimit: 0,
//     connectTimeout: 10000 // Увеличение времени ожидания подключения до 10 секунд
// });

// Проверка доступности сервера базы данных
async function testConnection() {
    let connection;
    try {
        connection = await pool.getConnection();
        logSuccess(`\nУспешное подключение к базе данных!\n`);
    } catch (error) {
        logError(`\nНе удалось подключиться к базе данных: ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

testConnection();

// For RESTART DataBase apy_info:  
// const dropTableQuery = `DROP TABLE IF EXISTS apy_info;`;
//         await pool.query(dropTableQuery);
//         console.log('Таблица успешно удалена');

// For RESTART DataBase incomDSFfromEveryOne:  
// const dropTableQuery = `DROP TABLE IF EXISTS incomDSFfromEveryOne;`;
//         await pool.query(dropTableQuery);
//         console.log('Таблица успешно удалена');

// For RESTART DataBase settings:  
// const dropTableQuery = `DROP TABLE IF EXISTS settings;`;
//         await pool.query(dropTableQuery);
//         console.log('Таблица успешно удалена');

// For RESTART DataBase wallet_info: 
// const dropTableQuery = `DROP TABLE IF EXISTS wallet_info;`;
//         await pool.query(dropTableQuery);
//         console.log('Таблица успешно удалена');

// For RESTART DataBase contract_events: 
const dropTableQuery = `DROP TABLE IF EXISTS contract_events;`;
        await pool.query(dropTableQuery);
        console.log('Таблица успешно удалена');

// For RESTART DataBase personal_yield_rate: 
// const dropTableQuery = `DROP TABLE IF EXISTS personal_yield_rate;`;
//         await pool.query(dropTableQuery);
//         console.log('Таблица успешно удалена');

//
//
// Создание Таблиц
//
//

// Таблица wallet_info
const createWalletTableQuery = `
    CREATE TABLE IF NOT EXISTS wallet_info (
        id INT AUTO_INCREMENT PRIMARY KEY,
        wallet_address VARCHAR(255) NOT NULL UNIQUE,
        user_deposits DECIMAL(36, 2) NOT NULL,
        dsf_lp_balance DECIMAL(36, 18) NOT NULL,
        ratio_user DECIMAL(36, 16) NOT NULL,
        available_to_withdraw DECIMAL(36, 6) NOT NULL,
        cvx_share DECIMAL(36, 18) NOT NULL,
        cvx_cost DECIMAL(36, 6) NOT NULL,
        crv_share DECIMAL(36, 18) NOT NULL,
        crv_cost DECIMAL(36, 6) NOT NULL,
        annual_yield_rate DECIMAL(36, 6) NOT NULL,
        eth_spent DECIMAL(36, 18) NOT NULL,
        usd_spent DECIMAL(36, 2) NOT NULL,
        eth_saved DECIMAL(36, 18) NOT NULL,
        usd_saved DECIMAL(36, 2) NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
`;
async function initializeDatabaseWallet() {
    let connection;
    try {
        connection = await pool.getConnection();
        await connection.query(createWalletTableQuery);
        logSuccess(`\nTable 'wallet_info' checked/created successfully.\n`);
    } catch (error) {
        logError(`\nFailed to create 'wallet_info' table: ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

initializeDatabaseWallet();

// Таблица для хранениния номера последнего проверенного блока
const createSettingsTableQuery = `
    CREATE TABLE IF NOT EXISTS settings (
        id INT AUTO_INCREMENT PRIMARY KEY,
        setting_key VARCHAR(255) NOT NULL UNIQUE,
        value VARCHAR(255) NOT NULL
    );
`;

async function initializeDatabaseSettings() {
    let connection;
    try {
        connection = await pool.getConnection();
        await connection.query(createSettingsTableQuery);
        logSuccess(`\nTable 'Settings' checked/created successfully.\n`);
    } catch (error) {
        logError(`\nFailed to create 'settings' table: ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

initializeDatabaseSettings();

//NEW
// Таблица apy_info
const createApyTableQuery = `
    CREATE TABLE IF NOT EXISTS apy_info (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME NOT NULL UNIQUE,
        apy DECIMAL(10, 4) NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
`;

//NEW
async function initializeDatabaseApy() {
    let connection;
    try {
        connection = await pool.getConnection();
        await connection.query(createApyTableQuery);
        logSuccess(`\nTable 'apy_info' checked/created successfully.\n`);
    } catch (error) {
        logError(`\nFailed to create 'apy_info' table: ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

//NEW
initializeDatabaseApy();

//NEW 
// Таблица wallet_events
const createEventsTableQuery = `
    CREATE TABLE IF NOT EXISTS wallet_events (
        id INT AUTO_INCREMENT PRIMARY KEY,
        event VARCHAR(255),
        eventDate TIMESTAMP,
        transactionCostEth DECIMAL(36, 18),
        transactionCostUsd DECIMAL(36, 2),
        returnValues JSON,
        wallet_address VARCHAR(255),
        INDEX(wallet_address)
    );
`;

//NEW
async function initializeWalletEventsTable() {
    let connection;
    try {
        connection = await pool.getConnection();
        await connection.query(createEventsTableQuery);
        logSuccess(`\nTable 'wallet_events' checked/created successfully.\n`);
    } catch (error) {
        logError(`\nFailed to create 'wallet_events' table: ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

//NEW
initializeWalletEventsTable();

//NEW
// Таблица unique_depositors список всех Депозиторов
const createUniqueDepositorsTableQuery = `
    CREATE TABLE IF NOT EXISTS unique_depositors (
        id INT AUTO_INCREMENT PRIMARY KEY,
        depositor_address VARCHAR(255) NOT NULL UNIQUE
    );
`;

async function initializeUniqueDepositorsTable() {
    let connection;
    try {
        connection = await pool.getConnection();
        await connection.query(createUniqueDepositorsTableQuery);
        logSuccess(`\nTable 'unique_depositors' checked/created successfully.\n`);
    } catch (error) {
        logError(`\nFailed to create 'unique_depositors' table: ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

initializeUniqueDepositorsTable();

//NEW 
// Таблица contract_events 
const createAllContractEventsTableQuery = `
    CREATE TABLE IF NOT EXISTS contract_events (
        id INT AUTO_INCREMENT PRIMARY KEY,
        event VARCHAR(255),
        eventDate TIMESTAMP,
        transactionCostEth DECIMAL(36, 18),
        transactionCostUsd DECIMAL(36, 2),
        returnValues JSON,
        blockNumber BIGINT,
        transactionHash VARCHAR(255),
        INDEX(event)
    );
`;

//NEW
async function initializeAllContractEventsTable() {
    let connection;
    try {
        connection = await pool.getConnection();
        await connection.query(createAllContractEventsTableQuery);
        logSuccess(`\nTable 'contract_events' checked/created successfully.\n`);
    } catch (error) {
        logError(`\nFailed to create 'contract_events' table: ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

// Initialize database
//NEW
initializeAllContractEventsTable();

// Таблица availableToWithdraw 
const createAvailableToWithdrawTableQuery = `
    CREATE TABLE IF NOT EXISTS availableToWithdraw (
        id INT AUTO_INCREMENT PRIMARY KEY,
        event VARCHAR(255) NOT NULL,
        eventDate DATETIME NOT NULL,
        blockNumber BIGINT NOT NULL,
        transactionHash VARCHAR(255) NOT NULL,
        availableToWithdraw DECIMAL(18, 6) NOT NULL
    );
`;

async function initializeAvailableToWithdrawTable() {
    let connection;
    try {
        connection = await pool.getConnection();
        await connection.query(createAvailableToWithdrawTableQuery);
        logSuccess(`\nTable 'availableToWithdraw' created successfully.\n`);
    } catch (error) {
        logError(`\nFailed to create 'availableToWithdraw' table: ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

initializeAvailableToWithdrawTable();

// v3
// Таблица incomDSFfromEveryOne для хранения информации о доходе DSF с каждого кошелька
const createIncomDSFfromEveryOneTableQuery = `
    CREATE TABLE IF NOT EXISTS incomDSFfromEveryOne (
        id INT AUTO_INCREMENT PRIMARY KEY,
        wallet_address VARCHAR(255) NOT NULL,
        incomeDSF DECIMAL(36, 18) NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE(wallet_address)
    );
`;


async function initializeDatabaseIncomDSF() {
    let connection;
    try {
        connection = await pool.getConnection();
        await connection.query(createIncomDSFfromEveryOneTableQuery);
        console.log(`Table 'incomDSFfromEveryOne' checked/created successfully.`);
    } catch (error) {
        console.error(`Failed to create 'incomDSFfromEveryOne' table: ${error}`);
    } finally {
        if (connection) connection.release();
    }
}

initializeDatabaseIncomDSF();

// Таблица weighted_yield_rate для хранения средневзвешенной ставки дохода
const createWeightedYieldRateTableQuery = `
    CREATE TABLE IF NOT EXISTS weighted_yield_rate (
        id INT AUTO_INCREMENT PRIMARY KEY,
        depositor_address VARCHAR(255) NOT NULL,
        date DATE NOT NULL,
        weighted_yield_rate DECIMAL(18, 8) NOT NULL,
        total_deposit DECIMAL(18, 8) NOT NULL,
        UNIQUE KEY unique_depositor_date (depositor_address, date)
    );
`;

async function initializeWeightedYieldRateTable() {
    let connection;
    try {
        connection = await pool.getConnection();
        await connection.query(createWeightedYieldRateTableQuery);
        logSuccess(`\nTable 'weighted_yield_rate' checked/created successfully.\n`);
    } catch (error) {
        logError(`\nFailed to create 'weighted_yield_rate' table: ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

initializeWeightedYieldRateTable();

// уже не нужна, удалить
// Таблица personal_yield_rate для хранения персональной ставки доходности
const createPersonalYieldRateTableQuery = `
    CREATE TABLE IF NOT EXISTS personal_yield_rate (
        id INT AUTO_INCREMENT PRIMARY KEY,
        depositor_address VARCHAR(255) NOT NULL,
        date DATE NOT NULL,
        daily_income DECIMAL(18, 8) NOT NULL,
        daily_yield_rate DECIMAL(18, 8) NOT NULL,
        annual_apy DECIMAL(18, 8) NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY unique_depositor_date (depositor_address, date)
    );
`;

async function initializePersonalYieldRateTable() {
    let connection;
    try {
        connection = await pool.getConnection();
        await connection.query(createPersonalYieldRateTableQuery);
        logSuccess(`\nTable 'personal_yield_rate' checked/created successfully.\n`);
    } catch (error) {
        logError(`\nFailed to create 'personal_yield_rate' table: ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

initializePersonalYieldRateTable();


// Connect To Web3 Provider

const providers = process.env.PROVIDERS.split(',');

let providerIndex = 0;
let web3;

async function connectToWeb3Provider() {
    try {
        const selectedProvider = providers[providerIndex];
        web3 = new Web3(selectedProvider);
        await web3.eth.net.getId();
        console.log(`Connected to Ethereum network using provider: ${selectedProvider}`);
    } catch (error) {
        logError(`Failed to connect to provider ${providers[providerIndex]}: ${error}`);
        //Go to the next provider
        providerIndex = (providerIndex + 1) % providers.length;
        await connectToWeb3Provider(); // Recursively try to connect to the next ISP
    }
}

connectToWeb3Provider();

// v2
// Функция для проверки провайдеров
async function checkWeb3Provider(provider) {
    try {
        const response = await axios.post(provider, {
            jsonrpc: "2.0",
            method: "eth_blockNumber",
            params: [],
            id: 1
        });

        if (response.data && response.data.result) {
            logSuccess(`VALID   - Web3 Provider ${provider}`);
            return true;
        } else {
            const errorMessage = response.statusText || "Unknown error";
            logError(`INVALID - Provider ${provider} : ${errorMessage}`);
            return false;
        }
    } catch (error) {
        logError(`An error occurred while checking the provider ${provider} : ${error.message}`);
        return false;
    }
}

const etherscanApiKeys = process.env.ETHERSCAN_API_KEYS 
    ? process.env.ETHERSCAN_API_KEYS.split(',')
    : [];

let etherscanApiKeyIndex = 0;

// Функция для переключения API ключей etherscan
function getNextEtherscanApiKey() {
    const apiKey = etherscanApiKeys[etherscanApiKeyIndex];
    etherscanApiKeyIndex = (etherscanApiKeyIndex + 1) % etherscanApiKeys.length;
    console.log(`Next Etherscan API Key: ${apiKey}`);
    return apiKey;
}

// v2
// Функция для проверки API ключей etherscan
async function checkEtherscanApiKey(apiKey) {
    try {
        const response = await axios.get('https://api.etherscan.io/api', {
            params: {
                module: 'stats',
                action: 'ethprice',
                apikey: apiKey
            }
        });

        if (response.data && response.data.status === '1') {
            logSuccess(`VALID   - Etherscan API key ${apiKey}`);
            return true;
        } else {
            logError(`INVALID - Etherscan API key ${apiKey} : ${response.data.message}`);
            return false;
        }
    } catch (error) {
        logError(`An error occurred while checking the API key ${apiKey} : ${error}`);
        return false;
    }
}

async function checkAllApiKeys() {
    
    logWarning(`\nCheck Etherscan Api Keys\n`);
    for (const apiKey of etherscanApiKeys) {
        const isValid = await checkEtherscanApiKey(apiKey);
        //console.log(`API key ${apiKey} validity: ${isValid}`);
    }

    logWarning(`\nCheck Web3 Providers Api Keys\n`);
    for (const provider of providers) {
        const isValid = await checkWeb3Provider(provider);
        //console.log(`Provider ${provider} validity: ${isValid}`);
    }
}

// Функция для повторного выполнения запроса с экспоненциальной задержкой
const retry = async (fn, retries = 3, delay = 200) => {
    try {
        return await fn();
    } catch (error) {
        if (retries > 0) {
            logWarning(`Retrying... attempts left: ${retries}`);
            providerIndex = (providerIndex + 1) % providers.length;
            await connectToWeb3Provider(); // Обновление провайдера перед повторной попыткой
            await new Promise(res => setTimeout(res, delay));
            return retry(fn, retries - 1, delay * 2);
        } else {
            throw error;
        }
    }
};

// Функция для повторного выполнения запроса с экспоненциальной задержкой
const retryEtherscan = async (fn, retries = 5, delay = 200) => {
    try {
        return await fn();
    } catch (error) {
        if (retries > 0) {
            logWarning(`Retrying... attempts left: ${retries}`);
            etherscanApiKeyIndex = (etherscanApiKeyIndex + 1) % etherscanApiKeys.length;
            //await getNextEtherscanApiKey(); // Обновление провайдера перед повторной попыткой
            await new Promise(res => setTimeout(res, delay));
            return retryEtherscan(fn, retries - 1, delay * 2);
        } else {
            throw error;
        }
    }
};

const contractDSF = new web3.eth.Contract(dsfABI, contractsLib.DSFmain);
const contractDSFStrategy = new web3.eth.Contract(dsfStrategyABI, contractsLib.DSFStrategy);
const ratioContract = new web3.eth.Contract(dsfRatioABI, contractsLib.DSFratio);
const cvxRewardsContract = new web3.eth.Contract(crvRewardsABI, contractsLib.crvRewards);
const routerContract = new web3.eth.Contract(uniswapRouterABI, contractsLib.uniswapV2Router);
const config_crvContract = new web3.eth.Contract(crvABI, contractsLib.CRV);
const config_cvxContract = new web3.eth.Contract(cvxABI, contractsLib.CVX);

const crvToUsdtPath = [contractsLib.CRV,contractsLib.WETH,contractsLib.USDT];
const cvxToUsdtPath = [contractsLib.CVX,contractsLib.WETH,contractsLib.USDT];

const addressesStrategy = [
    contractsLib.DSFStrategy
];

//
//
// По балансам пользователей 
//
//
async function getWalletData(walletAddress_) {
    if (!walletAddress_) {
        throw new Error("\nwalletAddress is not defined");
    }
    const walletAddress = normalizeAddress(walletAddress_);
    console.log('\nNormalized Address      :', walletAddress);

    // считаем сэкономленое и потраченое кошельком на транзакциях
    const resultSavingsAndSpending = await calculateSavingsAndSpending(walletAddress_);
    const ethSpent = resultSavingsAndSpending.totalEthSpent;
    const usdSpent = resultSavingsAndSpending.totalUsdSpent;
    const ethSaved = resultSavingsAndSpending.totalEthSaved;
    const usdSaved = resultSavingsAndSpending.totalUsdSaved;

    let ratioUser_ = 0; // Установите значение по умолчанию на случай ошибки

    try {
        ratioUser_ = await retry(() => ratioContract.methods.calculateLpRatio(walletAddress).call());
        console.log('ratioUser_:',ratioUser_);
    } catch (error) {
        logError(`Error occurred while fetching ratio : ${error}`);
        ratioUser_ = 0; // Установка значения 0 в случае ошибки
        console.log('ratioUser_:',ratioUser_);
    }

    if (ratioUser_ === 0) {
        logWarning(`userDeposits       USDT : 0`);
        logWarning(`dsfLpBalance     DSF LP : 0`);
        logWarning(`ratioUser             % : 0`);
        logWarning(`availableWithdraw  USDT : 0`);
        logWarning(`cvxShare            CVX : 0`);
        logWarning(`cvxCost            USDT : 0`);
        logWarning(`crvShare            CRV : 0`);
        logWarning(`crvCost            USDT : 0`);
        logWarning(`annualYieldRate       % : 0`);
        logWarning(`ethSpent            ETH : ${ethSpent}`);
        logWarning(`usdSpent              $ : ${usdSpent}`);
        logWarning(`ethSaved            ETH : ${ethSaved}`);
        logWarning(`usdSaved              $ : ${usdSaved}`);
        return {
            userDeposits: 0,
            dsfLpBalance: 0,
            safeRatioUser: 0,
            availableToWithdraw: 0,
            cvxShare: 0,
            cvxCost: 0,
            crvShare: 0,
            crvCost: 0,
            annualYieldRate: 0,
            ethSpent,
            usdSpent,
            ethSaved,
            usdSaved
        };
    }

    let availableToWithdraw_;

    try {
        availableToWithdraw_ = await retry(() => contractDSFStrategy.methods.calcWithdrawOneCoin(ratioUser_, 2).call());
        console.log('availableToWithdraw_   :',availableToWithdraw_);
    } catch (error) {
        logError("Error occurred while fetching available to withdraw:", error);
        availableToWithdraw_ = 0; // Установка значения 0 в случае ошибки
        console.log('availableToWithdraw_   :',availableToWithdraw_);
    }

    let dsfLpBalance_;

    try {
        dsfLpBalance_ = await retry(() => contractDSF.methods.balanceOf(walletAddress).call());
        console.log('dsfLpBalance_          :',dsfLpBalance_);
    } catch (error) {
        logError("Error occurred while fetching DSF LP balance:", error);
        dsfLpBalance_ = 0; // Установка значения 0 в случае ошибки
        console.log('dsfLpBalance_          :',dsfLpBalance_);
    }

    try {
        const availableToWithdraw = Number(availableToWithdraw_) / 1e6
        const dsfLpBalance = (Number(dsfLpBalance_) / 1e18).toPrecision(18);

        // const response = await axios.get(`https://api.dsf.finance/deposit/${walletAddress}`);
        // console.log('response:',response.data);

        const userDeposits = await calculateCurrentDeposit(walletAddress);
        console.log('userDeposits:',userDeposits);
        
        const crvEarned = await retry(() => cvxRewardsContract.methods.earned(contractsLib.DSFStrategy).call());
        console.log('crvEarned:',crvEarned);
        const cvxTotalCliffs = await retry(() => config_cvxContract.methods.totalCliffs().call());
        console.log('cvxTotalCliffs:',cvxTotalCliffs);
        const cvx_totalSupply = await retry(() => config_cvxContract.methods.totalSupply().call());
        console.log('cvx_totalSupply:',cvx_totalSupply);
        const cvx_reductionPerCliff = await retry(() => config_cvxContract.methods.reductionPerCliff().call());
        console.log('cvx_reductionPerCliff:',cvx_reductionPerCliff);
        const cvx_balanceOf = await retry(() => config_cvxContract.methods.balanceOf(contractsLib.DSFStrategy).call());
        console.log('cvx_balanceOf:',cvx_balanceOf);
        const crv_balanceOf = await retry(() => config_crvContract.methods.balanceOf(contractsLib.DSFStrategy).call());
        console.log('crv_balanceOf:',crv_balanceOf);
        const cvxRemainCliffs = cvxTotalCliffs - cvx_totalSupply / cvx_reductionPerCliff;
        console.log('cvxRemainCliffs:',cvxRemainCliffs);
        const amountInCVX = (crvEarned * cvxRemainCliffs) / cvxTotalCliffs + cvx_balanceOf;
        console.log('amountInCVX:',amountInCVX);
        const amountInCRV = crvEarned + crv_balanceOf;
        console.log('amountInCRV:',amountInCRV);
        
        let crvShare = 0;
        let cvxShare = 0;
        let crvCost = 0;
        let cvxCost = 0;

        const crvShare_ = Math.trunc(Number(amountInCRV) * Number(ratioUser_) / 1e18 * 0.85); // ratioUser_ CRV
        const cvxShare_ = Math.trunc(Number(amountInCVX) * Number(ratioUser_) / 1e18 * 0.85); // ratioUser_ CVX
        console.log('crvShare_:',crvShare_);

        if (crvShare_ > 20000 && cvxShare_ > 20000) {
            const crvCost_Array = await retry(() => routerContract.methods.getAmountsOut(Math.trunc(crvShare_), crvToUsdtPath).call());
            const cvxCost_Array = await retry(() => routerContract.methods.getAmountsOut(Math.trunc(cvxShare_), cvxToUsdtPath).call());
            console.log('crvCost_Array:',crvCost_Array);
            crvCost = Number(crvCost_Array[crvCost_Array.length - 1]) / 1e6;
            console.log('crvCost:',crvCost);
            cvxCost = Number(cvxCost_Array[cvxCost_Array.length - 1]) / 1e6;
            console.log('cvxCost:',cvxCost);

            crvShare = Number(crvShare_) / 1e18;
            cvxShare = Number(cvxShare_) / 1e18;
            console.log('cvxShare:',cvxShare);
        } 

        const annualYieldRate = await calculateWeightedYieldRate(walletAddress, availableToWithdraw, cvxCost, crvCost, userDeposits);
        console.log('annualYieldRate:',annualYieldRate);

        const ratioUser = parseFloat(ratioUser_) / 1e16;
        console.log('ratioUser:',ratioUser);
        const safeRatioUser = (ratioUser ? parseFloat(ratioUser) : 0.0).toPrecision(16);
        console.log('safeRatioUser:',safeRatioUser);
        
        console.log("userDeposits       USDT : " + userDeposits);
        console.log("dsfLpBalance     DSF LP : " + dsfLpBalance);
        console.log("ratioUser             % : " + safeRatioUser);
        console.log("availableWithdraw  USDT : " + availableToWithdraw);
        console.log("cvxShare            CVX : " + cvxShare);
        console.log("cvxCost            USDT : " + cvxCost);
        console.log("crvShare            CRV : " + crvShare);
        console.log("crvCost            USDT : " + crvCost);
        console.log("annualYieldRate       % : " + annualYieldRate);
        console.log(`ethSpent            ETH : ${ethSpent}`);
        console.log(`usdSpent              $ : ${usdSpent}`);
        console.log(`ethSaved            ETH : ${ethSaved}`);
        console.log(`usdSaved              $ : ${usdSaved}`);

        return {
            userDeposits,
            dsfLpBalance,
            safeRatioUser,
            availableToWithdraw,
            cvxShare,
            cvxCost,
            crvShare,
            crvCost,
            annualYieldRate,
            ethSpent,
            usdSpent,
            ethSaved,
            usdSaved
        };
    } catch (error) {
        logError(`Error retrieving data for wallet: ${walletAddress} ${error}`);
        logWarning("userDeposits       USDT : 0");
        logWarning("dsfLpBalance     DSF LP : 0");
        logWarning("ratioUser             % : 0");
        logWarning("availableWithdraw  USDT : 0");
        logWarning("cvxShare            CVX : 0");
        logWarning("cvxCost            USDT : 0");
        logWarning("crvShare            CRV : 0");
        logWarning("crvCost            USDT : 0");
        logWarning("annualYieldRate       % : 0");
        logWarning(`ethSpent            ETH : 0`);
        logWarning(`usdSpent              $ : 0`);
        logWarning(`ethSaved            ETH : 0`);
        logWarning(`usdSaved              $ : 0`);
        return {
            userDeposits: 0,
            dsfLpBalance: 0,
            safeRatioUser: 0,
            availableToWithdraw: 0,
            cvxShare: 0,
            cvxCost: 0,
            crvShare: 0,
            crvCost: 0,
            annualYieldRate: 0,
            ethSpent: 0,
            usdSpent: 0,
            ethSaved: 0,
            usdSaved: 0
        };
    }
}

async function getWalletDataOptim(walletAddress_, cachedData) {
    if (!walletAddress_) {
        throw new Error("\nwalletAddress is not defined");
    }
    const walletAddress = normalizeAddress(walletAddress_);
    console.log('\nNormalized Address      :', walletAddress);

    // считаем сэкономленое и потраченое кошельком на транзакциях
    const resultSavingsAndSpending = await calculateSavingsAndSpending(walletAddress_);
    const ethSpent = resultSavingsAndSpending.totalEthSpent;
    const usdSpent = resultSavingsAndSpending.totalUsdSpent;
    const ethSaved = resultSavingsAndSpending.totalEthSaved;
    const usdSaved = resultSavingsAndSpending.totalUsdSaved;

    let ratioUser_ = 0;

    try {
        ratioUser_ = await retry(() => ratioContract.methods.calculateLpRatio(walletAddress).call());
        console.log('ratioUser_:', ratioUser_);
    } catch (error) {
        logError("Error occurred while fetching ratio"); //, error);
        ratioUser_ = 0;
        console.log('ratioUser_:', ratioUser_);
    }

    if (ratioUser_ === 0) {
        logWarning(`userDeposits       USDT : 0`);
        logWarning(`dsfLpBalance     DSF LP : 0`);
        logWarning(`ratioUser             % : 0`);
        logWarning(`availableWithdraw  USDT : 0`);
        logWarning(`cvxShare            CVX : 0`);
        logWarning(`cvxCost            USDT : 0`);
        logWarning(`crvShare            CRV : 0`);
        logWarning(`crvCost            USDT : 0`);
        logWarning(`annualYieldRate       % : 0`);
        logWarning(`ethSpent            ETH : ${ethSpent}`);
        logWarning(`usdSpent              $ : ${usdSpent}`);
        logWarning(`ethSaved            ETH : ${ethSaved}`);
        logWarning(`usdSaved              $ : ${usdSaved}`);
        return {
            userDeposits: 0,
            dsfLpBalance: 0,
            safeRatioUser: 0,
            availableToWithdraw: 0,
            cvxShare: 0,
            cvxCost: 0,
            crvShare: 0,
            crvCost: 0,
            annualYieldRate: 0,
            ethSpent,
            usdSpent,
            ethSaved,
            usdSaved
        };
    }

    let availableToWithdraw_;

    try {
        availableToWithdraw_ = await retry(() => contractDSFStrategy.methods.calcWithdrawOneCoin(ratioUser_, 2).call());
        //console.log('availableToWithdraw_:', availableToWithdraw_);
    } catch (error) {
        logError("Error occurred while fetching available to withdraw"); //, error);
        availableToWithdraw_ = 0;
        //console.log('availableToWithdraw_:', availableToWithdraw_);
    }

    let dsfLpBalance_;

    try {
        dsfLpBalance_ = await retry(() => contractDSF.methods.balanceOf(walletAddress).call());
        //console.log('dsfLpBalance_:', dsfLpBalance_);
    } catch (error) {
        logError("Error occurred while fetching DSF LP balance"); //, error);
        dsfLpBalance_ = 0;
        //console.log('dsfLpBalance_:', dsfLpBalance_);
    }

    try {
        const availableToWithdraw = Number(availableToWithdraw_) / 1e6;
        const dsfLpBalance = (Number(dsfLpBalance_) / 1e18).toPrecision(18);

        const userDeposits = await calculateCurrentDeposit(walletAddress);

        let crvShare = 0;
        let cvxShare = 0;
        let crvCost = 0;
        let cvxCost = 0;

        const crvShare_ = Math.trunc(Number(cachedData.amountInCRV) * Number(ratioUser_) / 1e18 * 0.85);
        const cvxShare_ = Math.trunc(Number(cachedData.amountInCVX) * Number(ratioUser_) / 1e18 * 0.85);
        //console.log('crvShare_:', crvShare_);

        if (crvShare_ > 20000 && cvxShare_ > 20000) {
            const crvCost_Array = await retry(() => routerContract.methods.getAmountsOut(Math.trunc(crvShare_), crvToUsdtPath).call());
            const cvxCost_Array = await retry(() => routerContract.methods.getAmountsOut(Math.trunc(cvxShare_), cvxToUsdtPath).call());
            //console.log('crvCost_Array:', crvCost_Array);
            crvCost = Number(crvCost_Array[crvCost_Array.length - 1]) / 1e6;
            //console.log('crvCost:', crvCost);
            cvxCost = Number(cvxCost_Array[cvxCost_Array.length - 1]) / 1e6;
            //console.log('cvxCost:', cvxCost);

            crvShare = Number(crvShare_) / 1e18;
            cvxShare = Number(cvxShare_) / 1e18;
            //console.log('cvxShare:', cvxShare);
        }

        const annualYieldRate = await calculateWeightedYieldRate(walletAddress, availableToWithdraw, cvxCost, crvCost, userDeposits);
        
        const ratioUser = parseFloat(ratioUser_) / 1e16;
        //console.log('ratioUser:', ratioUser);
        const safeRatioUser = (ratioUser ? parseFloat(ratioUser) : 0.0).toPrecision(16);
        //console.log('safeRatioUser:', safeRatioUser);

        console.log("userDeposits       USDT : " + userDeposits);
        console.log("dsfLpBalance     DSF LP : " + dsfLpBalance);
        console.log("ratioUser             % : " + safeRatioUser);
        console.log("availableWithdraw  USDT : " + availableToWithdraw);
        console.log("cvxShare            CVX : " + cvxShare);
        console.log("cvxCost            USDT : " + cvxCost);
        console.log("crvShare            CRV : " + crvShare);
        console.log("crvCost            USDT : " + crvCost);
        console.log("annualYieldRate       % : " + annualYieldRate);
        console.log(`ethSpent            ETH : ${ethSpent}`);
        console.log(`usdSpent              $ : ${usdSpent}`);
        console.log(`ethSaved            ETH : ${ethSaved}`);
        console.log(`usdSaved              $ : ${usdSaved}`);

        return {
            userDeposits,
            dsfLpBalance,
            safeRatioUser,
            availableToWithdraw,
            cvxShare,
            cvxCost,
            crvShare,
            crvCost,
            annualYieldRate,
            ethSpent,
            usdSpent,
            ethSaved,
            usdSaved
        };
    } catch (error) {
        logError(`Error retrieving data for wallet: ${walletAddress} ${error}`);
        logWarning("userDeposits       USDT : 0");
        logWarning("dsfLpBalance     DSF LP : 0");
        logWarning("ratioUser             % : 0");
        logWarning("availableWithdraw  USDT : 0");
        logWarning("cvxShare            CVX : 0");
        logWarning("cvxCost            USDT : 0");
        logWarning("crvShare            CRV : 0");
        logWarning("crvCost            USDT : 0");
        logWarning("annualYieldRate       % : 0");
        logWarning(`ethSpent            ETH : 0`);
        logWarning(`usdSpent              $ : 0`);
        logWarning(`ethSaved            ETH : 0`);
        logWarning(`usdSaved              $ : 0`);
        return {
            userDeposits: 0,
            dsfLpBalance: 0,
            safeRatioUser: 0,
            availableToWithdraw: 0,
            cvxShare: 0,
            cvxCost: 0,
            crvShare: 0,
            crvCost: 0,
            annualYieldRate: 0,
            ethSpent: 0,
            usdSpent: 0,
            ethSaved: 0,
            usdSaved: 0
        };
    }
}

// Функция для нормализации адреса Ethereum
function normalizeAddress(address) {
    if (web3.utils.isAddress(address)) {
        return web3.utils.toChecksumAddress(address);
    } else {
        logError(`Invalid Ethereum address: ${address}`);
        return null; // Возвращаем null, если адрес некорректен
    }
}

function serializeBigints(obj) {
    for (const key in obj) {
        if (typeof obj[key] === 'bigint') {
            obj[key] = obj[key].toString();
        } else if (obj[key] !== null && typeof obj[key] === 'object') {
            serializeBigints(obj[key]);
        }
    }
    return obj;
}

//
//
// Функция для расчета текущего депозита
async function calculateCurrentDeposit(walletAddress) {
    let connection;
    try {
        connection = await pool.getConnection();

        // Получаем все события для данного депозитора
        const [events] = await connection.query(
            `SELECT * FROM contract_events 
             WHERE JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.from')) = ? 
                OR JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.to')) = ? 
                OR JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.depositor')) = ? 
                OR JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.withdrawer')) = ? 
             ORDER BY eventDate ASC`,
            [walletAddress, walletAddress, walletAddress, walletAddress]
        );

        if (events.length === 0) {
            logInfo(`No events found for depositor: ${walletAddress}`);
            return 0;
        }

        let totalDepositedUSD = 0;
        let totalLpShares = 0;

        for (const event of events) {
            let returnValues = event.returnValues;

            // Проверка, если returnValues - строка, тогда парсинг
            if (typeof returnValues === 'string') {
                try {
                    returnValues = JSON.parse(returnValues);
                } catch (error) {
                    console.error(`Failed to parse returnValues for event: ${event.transactionHash}`, error);
                    continue;
                }
            }

            //logInfo(`Processing event: ${event.event} - ${event.transactionHash}`);

            if (event.event === 'Deposited') {
                
                if (totalDepositedUSD < 0) totalDepositedUSD = 0; // Защита от отрицательных значений

                // Проверяем наличие записи в availableToWithdraw для более точной суммы USD
                const [withdrawRecords] = await connection.query(
                    `SELECT availableToWithdraw FROM availableToWithdraw 
                     WHERE transactionHash = ? AND event = 'Deposited'`,
                    [event.transactionHash]
                );

                if (withdrawRecords.length > 0) {
                    totalDepositedUSD += parseFloat(withdrawRecords[0].availableToWithdraw);
                    //logInfo(`Deposited - Using availableToWithdraw value: ${withdrawRecords[0].availableToWithdraw}`);
                } else {
                    const depositedUSD = parseFloat(returnValues.amounts.DAI) + parseFloat(returnValues.amounts.USDC) + parseFloat(returnValues.amounts.USDT);
                    totalDepositedUSD += depositedUSD - (depositedUSD * 0.0016); // Вычитаем 0.16% комиссии
                    //logInfo(`Deposited - Calculated value: ${depositedUSD - (depositedUSD * 0.0016)}`);
                }
                totalLpShares += parseFloat(returnValues.lpShares);
                logInfo(`Updated totalDepositedUSD: ${totalDepositedUSD}, totalLpShares: ${totalLpShares}`);
            } else if (event.event === 'Transfer') {

                if (totalDepositedUSD < 0) totalDepositedUSD = 0; // Защита от отрицательных значений

                // Проверяем наличие записи в availableToWithdraw для более точной суммы USD
                const [withdrawRecords] = await connection.query(
                    `SELECT availableToWithdraw FROM availableToWithdraw 
                     WHERE transactionHash = ? AND event = 'Transfer'`,
                    [event.transactionHash]
                );

                const usdValue = withdrawRecords.length > 0 
                    ? parseFloat(withdrawRecords[0].availableToWithdraw) 
                    : parseFloat(returnValues.usdValue);
                const lpValue = parseFloat(returnValues.value);

                if (returnValues.from === walletAddress) {
                    totalDepositedUSD -= usdValue;
                    totalLpShares -= lpValue;

                    if (totalDepositedUSD < 0) totalDepositedUSD = 0; // Защита от отрицательных значений
                    if (totalLpShares < 0) totalLpShares = 0; // Защита от отрицательных значений

                    //logInfo(`Transfer - Sent: ${usdValue} USD, ${lpValue} LP`);
                } else if (returnValues.to === walletAddress) {
                    totalDepositedUSD += usdValue;
                    totalLpShares += lpValue;
                    //logInfo(`Transfer - Received: ${usdValue} USD, ${lpValue} LP`);
                }
                logInfo(`Updated totalDepositedUSD: ${totalDepositedUSD}, totalLpShares: ${totalLpShares}`);
            } else if (event.event === 'Withdrawn') {
                const [withdrawRecords] = await connection.query(
                    `SELECT availableToWithdraw FROM availableToWithdraw 
                     WHERE transactionHash = ? AND event = 'Withdrawn'`,
                    [event.transactionHash]
                );

                const withdrawnLpShares = parseFloat(returnValues.lpShares);
                const sharePercentage = withdrawnLpShares / totalLpShares;

                if (withdrawRecords.length > 0) {
                    const withdrawnUSD = parseFloat(withdrawRecords[0].availableToWithdraw);
                    totalDepositedUSD -= withdrawnUSD;
                    //logInfo(`Withdrawn - Using availableToWithdraw value: ${withdrawnUSD}`);
                } else {
                    const withdrawnUSD = totalDepositedUSD * sharePercentage;
                    totalDepositedUSD -= withdrawnUSD;
                    //logInfo(`Withdrawn - Calculated value: ${withdrawnUSD}`);
                }

                if (totalDepositedUSD < 0) totalDepositedUSD = 0; // Защита от отрицательных значений
                if (totalLpShares < 0) totalLpShares = 0; // Защита от отрицательных значений

                totalLpShares -= withdrawnLpShares;
                logInfo(`Updated totalDepositedUSD: ${totalDepositedUSD}, totalLpShares: ${totalLpShares}`);
            }
        }

        logSuccess(`Calculated current deposit for ${walletAddress} is ${totalDepositedUSD}`);
        return totalDepositedUSD;
    } catch (error) {
        logError("Failed to calculate current deposit:", error);
        return 0;
    } finally {
        if (connection) connection.release();
    }
}

// Функция для расчета средневзвешенной ставки дохода
async function calculateWeightedYieldRate(walletAddress, availableToWithdraw, cvxCost, crvCost, userDeposits) {
    let connection;
    try {
        connection = await pool.getConnection();

        // Получаем все события для данного депозитора
        const [events] = await connection.query(
            `SELECT * FROM contract_events 
             WHERE JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.from')) = ? 
                OR JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.to')) = ? 
                OR JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.depositor')) = ? 
                OR JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.withdrawer')) = ? 
             ORDER BY eventDate ASC`,
            [walletAddress, walletAddress, walletAddress, walletAddress]
        );

        if (events.length === 0) {
            logInfo(`События не найдены для депозитора: ${walletAddress}`);
            return 0;
        }

        let totalDepositedUSD = 0;
        let totalLpShares = 0;
        let weightedDepositDays = 0;
        let lastEventDate = null;

        for (const event of events) {
            let returnValues = event.returnValues;

            // Проверка, если returnValues - строка, тогда парсинг
            if (typeof returnValues === 'string') {
                try {
                    returnValues = JSON.parse(returnValues);
                } catch (error) {
                    console.error(`Не удалось разобрать returnValues для события: ${event.transactionHash}`, error);
                    continue;
                }
            }

            const eventDate = new Date(event.eventDate);
            const eventDateOnly = eventDate.toISOString().split('T')[0];

            logInfo(`Обработка события: ${event.event} - ${event.transactionHash} на ${eventDateOnly}`);

            let depositedUSD = 0;
            if (event.event === 'Deposited') {
                // Проверяем наличие записи в availableToWithdraw для более точной суммы USD
                const [withdrawRecords] = await connection.query(
                    `SELECT availableToWithdraw FROM availableToWithdraw 
                     WHERE transactionHash = ? AND event = 'Deposited'`,
                    [event.transactionHash]
                );

                if (withdrawRecords.length > 0) {
                    depositedUSD = parseFloat(withdrawRecords[0].availableToWithdraw);
                    logInfo(`${event.event} - Использование значения availableToWithdraw: ${withdrawRecords[0].availableToWithdraw}`);
                } else {
                    depositedUSD = parseFloat(returnValues.amounts.DAI) + parseFloat(returnValues.amounts.USDC) + parseFloat(returnValues.amounts.USDT);
                    depositedUSD -= depositedUSD * 0.0016; // Вычитаем 0.16% из суммы депозита
                    logInfo(`${event.event} - Рассчитанная сумма: ${depositedUSD} после вычета 0.16%`);
                }

                if (lastEventDate) {
                    const daysActive = (eventDate - lastEventDate) / (1000 * 60 * 60 * 24);
                    weightedDepositDays += totalDepositedUSD * daysActive;
                    logInfo(`Добавлено ${daysActive} активных дней для totalDepositedUSD: ${totalDepositedUSD}, взвешенные дни депозита теперь: ${weightedDepositDays}`);
                }

                totalDepositedUSD += depositedUSD;
                totalLpShares += parseFloat(returnValues.lpShares);
                lastEventDate = eventDate;
                logInfo(`Обновлено totalDepositedUSD: ${totalDepositedUSD}, totalLpShares: ${totalLpShares}, взвешенные дни депозита: ${weightedDepositDays}`);
            } else if (event.event === 'Transfer') {
                let usdValue = 0;
                const lpValue = parseFloat(returnValues.value);

                // Проверяем наличие записи в availableToWithdraw для более точной суммы USD
                const [withdrawRecords] = await connection.query(
                    `SELECT availableToWithdraw FROM availableToWithdraw 
                     WHERE transactionHash = ? AND event = 'Transfer'`,
                    [event.transactionHash]
                );

                usdValue = withdrawRecords.length > 0 
                    ? parseFloat(withdrawRecords[0].availableToWithdraw) 
                    : parseFloat(returnValues.usdValue);

                if (returnValues.from === walletAddress) {
                    if (lastEventDate) {
                        const daysActive = (eventDate - lastEventDate) / (1000 * 60 * 60 * 24);
                        weightedDepositDays += totalDepositedUSD * daysActive;
                        logInfo(`Добавлено ${daysActive} активных дней для totalDepositedUSD: ${totalDepositedUSD}, взвешенные дни депозита теперь: ${weightedDepositDays}`);
                    }

                    totalDepositedUSD -= usdValue;
                    totalLpShares -= lpValue;

                    if (totalDepositedUSD < 0) totalDepositedUSD = 0;
                    if (totalLpShares < 0) totalLpShares = 0;

                    if (lastEventDate) {
                        const daysActive = (eventDate - lastEventDate) / (1000 * 60 * 60 * 24);
                        weightedDepositDays -= usdValue * daysActive;
                        logInfo(`Вычтено ${daysActive} активных дней для withdrawnUSD: ${usdValue}, взвешенные дни депозита теперь: ${weightedDepositDays}`);
                    }
                    logInfo(`Transfer - Sent: ${usdValue} USD, ${lpValue} LP`);
                } else if (returnValues.to === walletAddress) {
                    if (lastEventDate) {
                        const daysActive = (eventDate - lastEventDate) / (1000 * 60 * 60 * 24);
                        weightedDepositDays += totalDepositedUSD * daysActive;
                        logInfo(`Добавлено ${daysActive} активных дней для totalDepositedUSD: ${totalDepositedUSD}, взвешенные дни депозита теперь: ${weightedDepositDays}`);
                    }

                    totalDepositedUSD += usdValue;
                    totalLpShares += lpValue;
                    logInfo(`Transfer - Received: ${usdValue} USD, ${lpValue} LP`);
                }
                lastEventDate = eventDate;
                logInfo(`Обновлено totalDepositedUSD: ${totalDepositedUSD}, totalLpShares: ${totalLpShares}, взвешенные дни депозита: ${weightedDepositDays}`);
            } else if (event.event === 'Withdrawn') {
                let withdrawnUSD = 0;
                const withdrawnLpShares = parseFloat(returnValues.lpShares);
                const sharePercentage = withdrawnLpShares / totalLpShares;

                const [withdrawRecords] = await connection.query(
                    `SELECT availableToWithdraw FROM availableToWithdraw 
                     WHERE transactionHash = ? AND event = 'Withdrawn'`,
                    [event.transactionHash]
                );

                if (withdrawRecords.length > 0) {
                    withdrawnUSD = parseFloat(withdrawRecords[0].availableToWithdraw);
                    logInfo(`${event.event} - Использование значения availableToWithdraw: ${withdrawRecords[0].availableToWithdraw}`);
                } else {
                    withdrawnUSD = totalDepositedUSD * sharePercentage;
                    logInfo(`${event.event} - Рассчитанная сумма: ${withdrawnUSD}`);
                }

                if (lastEventDate) {
                    const daysActive = (eventDate - lastEventDate) / (1000 * 60 * 60 * 24);
                    weightedDepositDays += totalDepositedUSD * daysActive;
                    logInfo(`Добавлено ${daysActive} активных дней для totalDepositedUSD: ${totalDepositedUSD}, взвешенные дни депозита теперь: ${weightedDepositDays}`);
                }

                totalDepositedUSD -= withdrawnUSD;
                totalLpShares -= withdrawnLpShares;

                if (totalDepositedUSD < 0) totalDepositedUSD = 0;
                if (totalLpShares < 0) totalLpShares = 0;

                if (lastEventDate) {
                    const daysActive = (eventDate - lastEventDate) / (1000 * 60 * 60 * 24);
                    weightedDepositDays -= withdrawnUSD * daysActive;
                    logInfo(`Вычтено ${daysActive} активных дней для withdrawnUSD: ${withdrawnUSD}, взвешенные дни депозита теперь: ${weightedDepositDays}`);
                }

                if (totalDepositedUSD === 0) {
                    weightedDepositDays = 0;
                    logInfo(`Депозит стал равен нулю, обнуляем взвешенные дни депозита.`);
                }

                lastEventDate = eventDate;
                logInfo(`Обновлено totalDepositedUSD: ${totalDepositedUSD}, totalLpShares: ${totalLpShares}, взвешенные дни депозита: ${weightedDepositDays}`);
            }
        }

        // Добавляем текущие взвешенные дни депозита до текущей даты
        if (lastEventDate && totalDepositedUSD > 0) {
            const daysActive = (new Date() - lastEventDate) / (1000 * 60 * 60 * 24);
            weightedDepositDays += totalDepositedUSD * daysActive;
            logInfo(`Добавлено ${daysActive} активных дней для totalDepositedUSD: ${totalDepositedUSD} до сегодняшнего дня, взвешенные дни депозита теперь: ${weightedDepositDays}`);
        }

        const totalValue = availableToWithdraw + cvxCost + crvCost - userDeposits;
        logInfo(`Общая сумма из availableToWithdraw, cvxCost и crvCost - userDeposits: ${totalValue}`);

        if (weightedDepositDays === 0) {
            logWarning(`Взвешенные дни депозита равны нулю, возвращаем 0 для ${walletAddress}`);
            return 0;
        }

        const averageDailyRate = totalValue / weightedDepositDays;
        const annualYieldRate = averageDailyRate * 365 * 100;

        logInfo(`Средняя дневная ставка: ${averageDailyRate}`);
        logInfo(`Рассчитанная годовая доходность: ${annualYieldRate}`);

        // Вставляем или обновляем запись для текущего депозитора и даты
        const insertQuery = `
            INSERT INTO personal_yield_rate (depositor_address, date, daily_income, daily_yield_rate, annual_apy)
            VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
            daily_income = VALUES(daily_income),
            daily_yield_rate = VALUES(daily_yield_rate),
            annual_apy = VALUES(annual_apy)
        `;
        await connection.query(insertQuery, [walletAddress, new Date(), totalValue, averageDailyRate, annualYieldRate]);
        logInfo(`Вставлено/обновлено взвешенная ставка доходности для ${walletAddress} на ${new Date().toISOString().split('T')[0]}`);

        return annualYieldRate;
    } catch (error) {
        logError(`Не удалось рассчитать взвешенную ставку доходности: ${error}`);
        return 0;
    } finally {
        if (connection) connection.release();
    }
}

// Функция для вычисления потраченных и сэкономленных средств кошелька
async function calculateSavingsAndSpending(wallet) {
    
    const spendingQuery = `
    SELECT SUM(transactionCostEth) AS totalEthSpent, SUM(transactionCostUsd) AS totalUsdSpent
    FROM contract_events
    WHERE 
        (
            event IN ('CreatedPendingDeposit', 'CreatedPendingWithdrawal')
            OR 
            (event IN ('Withdrawn', 'Deposited', 'FailedWithdraw', 'FailedDeposit') AND JSON_EXTRACT(returnValues, '$.transaction_status') = 'Standard')
        )
        AND 
        (JSON_EXTRACT(returnValues, '$.depositor') = ? OR JSON_EXTRACT(returnValues, '$.withdrawer') = ?)
`;

    const savingsQuery = `
        SELECT SUM(transactionCostEth) AS totalEthOptimized, SUM(transactionCostUsd) AS totalUsdOptimized
        FROM contract_events
        WHERE (event = 'Withdrawn' OR event = 'Deposited')
        AND JSON_EXTRACT(returnValues, '$.transaction_status') = 'Optimized'
        AND (JSON_EXTRACT(returnValues, '$.depositor') = ? OR JSON_EXTRACT(returnValues, '$.withdrawer') = ?)
    `;

    const pendingCostsQuery = `
        SELECT SUM(transactionCostEth) AS totalEthPending, SUM(transactionCostUsd) AS totalUsdPending
        FROM contract_events
        WHERE (event = 'CreatedPendingDeposit' OR event = 'CreatedPendingWithdrawal')
        AND (JSON_EXTRACT(returnValues, '$.depositor') = ? OR JSON_EXTRACT(returnValues, '$.withdrawer') = ?)
    `;

    try {
        // Расчет потраченных средств
        console.log(`Executing spending query for wallet: ${wallet}`);
        const [spendingRows] = await pool.query(spendingQuery, [wallet, wallet]);
        console.log(`Spending query result: ${JSON.stringify(spendingRows)}`);
        const totalEthSpent = spendingRows[0].totalEthSpent || 0.0;
        const totalUsdSpent = spendingRows[0].totalUsdSpent || 0.0;
        console.log(`Total ETH spent: ${totalEthSpent}, Total USD spent: ${totalUsdSpent}`);

        // Расчет сэкономленных средств
        console.log(`Executing savings query for wallet: ${wallet}`);
        const [savingsRows] = await pool.query(savingsQuery, [wallet, wallet]);
        console.log(`Savings query result: ${JSON.stringify(savingsRows)}`);
        const totalEthOptimized = savingsRows[0].totalEthOptimized || 0.0;
        const totalUsdOptimized = savingsRows[0].totalUsdOptimized || 0.0;
        console.log(`Total ETH optimized: ${totalEthOptimized}, Total USD optimized: ${totalUsdOptimized}`);

        // Расчет потраченных средств
        console.log(`Executing pending costs query for wallet: ${wallet}`);
        const [pendingCostsRows] = await pool.query(pendingCostsQuery, [wallet, wallet]);
        console.log(`Pending costs query result: ${JSON.stringify(pendingCostsRows)}`);
        const totalEthPending = pendingCostsRows[0].totalEthPending || 0.0;
        const totalUsdPending = pendingCostsRows[0].totalUsdPending || 0.0;
        console.log(`Total ETH pending: ${totalEthPending}, Total USD pending: ${totalUsdPending}`);

        // Вычисление сэкономленных средств
        const totalEthSaved = totalEthOptimized - totalEthPending;
        const totalUsdSaved = totalUsdOptimized - totalUsdPending;
        console.log(`Total ETH saved: ${totalEthSaved}, Total USD saved: ${totalUsdSaved}`);

        const resultSavingsAndSpending = {
            totalEthSpent: totalEthSpent || 0.0,
            totalUsdSpent: totalUsdSpent || 0.0,
            totalEthSaved: totalEthSaved || 0.0,
            totalUsdSaved: totalUsdSaved || 0.0
        };

        console.log(`Result for wallet ${wallet}:`, resultSavingsAndSpending);
        return resultSavingsAndSpending;

    } catch (error) {
        console.error(`Failed to calculate savings and spending for wallet ${wallet}: ${error.message}`);
        throw error;
    }
}

// Функция для вычисления потраченных и сэкономленных средств кошелька, суммарных данных по всем кошелькам
async function calculateTotalSavingsAndSpending() {
    const query = `
        SELECT 
            SUM(eth_spent) AS totalEthSpent,
            SUM(usd_spent) AS totalUsdSpent,
            SUM(eth_saved) AS totalEthSaved,
            SUM(usd_saved) AS totalUsdSaved
        FROM wallet_info
    `;

    let connection;
    try {
        connection = await pool.getConnection();
        const [rows] = await connection.query(query);

        const totalEthSpent = rows[0].totalEthSpent || 0.0;
        const totalUsdSpent = rows[0].totalUsdSpent || 0.0;
        const totalEthSaved = rows[0].totalEthSaved || 0.0;
        const totalUsdSaved = rows[0].totalUsdSaved || 0.0;

        return {
            totalEthSpent,
            totalUsdSpent,
            totalEthSaved,
            totalUsdSaved
        };
    } catch (error) {
        console.error('Failed to calculate total savings and spending:', error);
        throw new Error('Failed to calculate total savings and spending');
    } finally {
        if (connection) connection.release();
    }
}


// Для getWalletDataOptim
async function updateWalletData(walletAddress, cachedData) {
    let connection;
    try {
        // Получение данных кошелька
        const walletData = await getWalletDataOptim(walletAddress, cachedData);

        //console.log(`Retrieved walletData: ${walletData}`);

        // Проверяем, что получены корректные данные
        if (!walletData || typeof walletData !== 'object') {
            throw new Error('Invalid wallet data retrieved');
        }

        //console.log('Retrieved walletData:', JSON.stringify(walletData));

        // Получение соединения с базой данных
        connection = await pool.getConnection();

        // Проверяем наличие кошелька в таблице wallet_info
        const [rows] = await connection.query('SELECT wallet_address FROM wallet_info WHERE wallet_address = ?', [walletAddress]);
        console.log("Existing wallet data from database:", rows);

        if (rows.length === 0) {
            // Если кошелька нет в базе, вставляем новую запись
            const insertQuery = `
                INSERT INTO wallet_info (
                    wallet_address,
                    user_deposits,
                    dsf_lp_balance,
                    ratio_user,
                    available_to_withdraw,
                    cvx_share,
                    cvx_cost,
                    crv_share,
                    crv_cost,
                    annual_yield_rate,
                    eth_spent,
                    usd_spent,
                    eth_saved,
                    usd_saved,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())

            `;

            const insertValues = [
                walletAddress,
                walletData.userDeposits,
                walletData.dsfLpBalance,
                walletData.safeRatioUser,
                walletData.availableToWithdraw,
                walletData.cvxShare,
                walletData.cvxCost,
                walletData.crvShare,
                walletData.crvCost,
                walletData.annualYieldRate,
                walletData.ethSpent,
                walletData.usdSpent,
                walletData.ethSaved,
                walletData.usdSaved
            ];

            //console.log('Insert query:', insertQuery);
            //console.log('Insert values:', insertValues);

            await connection.query(insertQuery, insertValues);
            logSuccess(`Data inserted for wallet : ${walletAddress}`);
        } else {
            // Запрос на обновление данных в базе данных
            const updateQuery = `
                UPDATE wallet_info SET
                user_deposits = ?,
                dsf_lp_balance = ?,
                ratio_user = ?,
                available_to_withdraw = ?,
                cvx_share = ?,
                cvx_cost = ?,
                crv_share = ?,
                crv_cost = ?,
                annual_yield_rate = ?,
                eth_spent = ?,
                usd_spent = ?,
                eth_saved = ?,
                usd_saved = ?,
                updated_at = NOW()
                WHERE wallet_address = ?
            `;

            // Параметры для запроса обновления
            const updateValues = [
                walletData.userDeposits,
                walletData.dsfLpBalance,
                walletData.safeRatioUser,
                walletData.availableToWithdraw,
                walletData.cvxShare,
                walletData.cvxCost,
                walletData.crvShare,
                walletData.crvCost,
                walletData.annualYieldRate,
                walletData.ethSpent,
                walletData.usdSpent,
                walletData.ethSaved,
                walletData.usdSaved,
                walletAddress
            ];

            //console.log('Update query:', updateQuery);
            //console.log('Update values:', updateValues);

            // Выполнение запроса обновления
            await connection.query(updateQuery, updateValues);
            logSuccess(`Data updated for wallet : ${walletAddress}`);
        }
    } catch (error) {
        logError(`Error updating wallet data for ${walletAddress}: ${error}`);
        throw error;
    } finally {
        // Освобождение соединения
        if (connection) connection.release();
    }
}

// Для getWalletData 
async function updateWalletDataSingl(walletAddress) {
    let connection;
    try {
        // Получение данных кошелька
        const walletData = await getWalletData(walletAddress);
        
        // Получение соединения с базой данных
        connection = await pool.getConnection();
        
        // Запрос на обновление данных в базе данных
        const updateQuery = `
            UPDATE wallet_info SET
            user_deposits = ?,
            dsf_lp_balance = ?,
            ratio_user = ?,
            available_to_withdraw = ?,
            cvx_share = ?,
            cvx_cost = ?,
            crv_share = ?,
            crv_cost = ?,
            annual_yield_rate = ?,
            eth_spent = ?,
            usd_spent = ?,
            eth_saved = ?,
            usd_saved = ?,
            updated_at = NOW()
            WHERE wallet_address = ?
        `;

        // Параметры для запроса обновления
        const values = [
            walletData.userDeposits,
            walletData.dsfLpBalance,
            walletData.safeRatioUser,
            walletData.availableToWithdraw,
            walletData.cvxShare,
            walletData.cvxCost,
            walletData.crvShare,
            walletData.crvCost,
            walletData.annualYieldRate,
            walletData.ethSpent,
            walletData.usdSpent,
            walletData.ethSaved,
            walletData.usdSaved,
            walletAddress
        ];

        // Выполнение запроса обновления
        await connection.query(updateQuery, values);
        logSuccess(`Data updated for wallet : ${walletAddress}`);
    } catch (error) {
        logError(`Error updating wallet data for ${walletAddress}:`, error);
        throw error;
    } finally {
        // Освобождение соединения
        if (connection) connection.release();
    }
}

async function updateAllWallets() {
    let connection;
    console.log("\nStarting update of all wallets...\n");
    try {
        connection = await pool.getConnection();
        
        // Получаем список адресов из таблицы unique_depositors
        const [wallets] = await connection.query('SELECT depositor_address AS wallet_address FROM unique_depositors');
        console.log('\n',wallets);

        // Получаем общие данные один раз
        const crvEarned = await retry(() => cvxRewardsContract.methods.earned(contractsLib.DSFStrategy).call());
        const cvxTotalCliffs = await retry(() => config_cvxContract.methods.totalCliffs().call());
        const cvx_totalSupply = await retry(() => config_cvxContract.methods.totalSupply().call());
        const cvx_reductionPerCliff = await retry(() => config_cvxContract.methods.reductionPerCliff().call());
        const cvx_balanceOf = await retry(() => config_cvxContract.methods.balanceOf(contractsLib.DSFStrategy).call());
        const crv_balanceOf = await retry(() => config_crvContract.methods.balanceOf(contractsLib.DSFStrategy).call());
        const cvxRemainCliffs = cvxTotalCliffs - cvx_totalSupply / cvx_reductionPerCliff;
        const amountInCVX = (crvEarned * cvxRemainCliffs) / cvxTotalCliffs + cvx_balanceOf;
        const amountInCRV = crvEarned + crv_balanceOf;

        const cachedData = {
            crvEarned,
            cvxTotalCliffs,
            cvx_totalSupply,
            cvx_reductionPerCliff,
            cvx_balanceOf,
            crv_balanceOf,
            cvxRemainCliffs,
            amountInCRV,
            amountInCVX
        };

        console.log(`\n${colors.yellow}${`Cached Data`}${colors.reset}\n`);
        console.log(`${colors.yellow}${`  crvEarned             : `}${colors.reset}${cachedData.crvEarned}`);
        console.log(`${colors.yellow}${`  cvxTotalCliffs        : `}${colors.reset}${cachedData.cvxTotalCliffs}`);
        console.log(`${colors.yellow}${`  cvx_totalSupply       : `}${colors.reset}${cachedData.cvx_totalSupply}`);
        console.log(`${colors.yellow}${`  cvx_reductionPerCliff : `}${colors.reset}${cachedData.cvx_reductionPerCliff}`);
        console.log(`${colors.yellow}${`  cvx_balanceOf         : `}${colors.reset}${cachedData.cvx_balanceOf}`);
        console.log(`${colors.yellow}${`  crv_balanceOf         : `}${colors.reset}${cachedData.crv_balanceOf}`);
        console.log(`${colors.yellow}${`  cvxRemainCliffs       : `}${colors.reset}${cachedData.cvxRemainCliffs}`);
        console.log(`${colors.yellow}${`  amountInCRV           : `}${colors.reset}${cachedData.amountInCRV}`);
        console.log(`${colors.yellow}${`  amountInCVX           : `}${colors.reset}${cachedData.amountInCVX}`);

        for (const wallet of wallets) {
            try {
                await updateWalletData(wallet.wallet_address, cachedData);
                await new Promise(resolve => setTimeout(resolve, 100)); // Задержка 100 мс между запросами
            } catch (error) {
                logError(`Error updating wallet ${wallet.wallet_address}: ${error.message}`);
                // Повторная попытка после задержки
                await new Promise(resolve => setTimeout(resolve, 2000));
                try {
                    await updateWalletData(wallet.wallet_address, cachedData);
                } catch (retryError) {
                    logError(`Retry failed for wallet ${wallet.wallet_address}: ${retryError.message}`);
                }
            }
        }
        logSuccess(`\nAll wallet data updated successfully.`);
    } catch (error) {
        logError(`\nError during initial wallet data update: ${error}`);
    } finally {
        if (connection) connection.release();
    }
}

// Проверить ???
app.post('/update/:walletAddress', async (req, res) => {
    const walletAddress_ = req.params.walletAddress.toLowerCase();
    if (!walletAddress_) {
        throw new Error("\nwalletAddress is not defined");
    }
    const walletAddress = normalizeAddress(walletAddress_);
    try {
        await updateWalletDataSingl(walletAddress);
        res.send({ message: 'Data updated successfully' });
    } catch (error) {
        logError('Failed to update data:', error);
        res.status(500).send('Failed to update wallet data');
    }
});

// Эндпоинт для получения данных всех кошельков
app.get('/wallets', async (req, res) => {
    let connection;

    try {
        // Получаем соединение с базой данных
        connection = await pool.getConnection();

        // Получаем все кошельки из базы данных
        const [rows] = await connection.query('SELECT * FROM wallet_info');

        // Отправляем список кошельков клиенту в формате JSON
        res.json(rows);
    } catch (error) {
        // Обработка ошибок при соединении или выполнении SQL-запроса
        logError('Database connection or operation failed:', error);
        res.status(500).send('Internal Server Error');
    } finally {
        // Освобождение соединения
        if (connection) {
            connection.release();
        }
    }
});

// Эндпоинт для получения потраченных и сэкономленных средств для конкретного кошелька
app.get('/wallet/savings/:wallet', async (req, res) => {
    const { wallet } = req.params;

    try {
        const resultSavingsAndSpending = await calculateSavingsAndSpending(wallet);
        res.json(resultSavingsAndSpending);
    } catch (error) {
        console.error(`Failed to calculate savings and spending for wallet ${wallet}: ${error.message}`);
        res.status(500).send('Failed to calculate savings and spending');
    }
});

// Эндпоинт для получения суммарных данных о потраченных и сэкономленных средствах со всех кошельков
app.get('/wallet/total-savings', async (req, res) => {
    try {
        const resultTotalSavingsAndSpending = await calculateTotalSavingsAndSpending();
        res.json(resultTotalSavingsAndSpending);
    } catch (error) {
        console.error(`Failed to calculate total savings and spending: ${error.message}`);
        res.status(500).send('Failed to calculate total savings and spending');
    }
});

// Эндпоинт для получения текущего депозита для конкретного кошелька
app.get('/current-deposit/:walletAddress', async (req, res) => {
    const walletAddress = req.params.walletAddress;

    try {
        const currentDeposit = await calculateCurrentDeposit(walletAddress);
        res.json({ walletAddress, currentDeposit });
    } catch (error) {
        logError(`Failed to calculate current deposit for ${walletAddress}: ${error}`);
        res.status(500).send('Internal Server Error');
    }
});

// Эндпоинт для получения всех данных для конкретного кошелька
app.get('/wallet/:walletAddress', async (req, res) => {

    connectToWeb3Provider();

    const walletAddress_ = req.params.walletAddress.toLowerCase();
    
    const walletAddress = normalizeAddress(walletAddress_);

    // Если адрес некорректный, возвращаем значения по умолчанию
    if (!walletAddress) {
        logError('Invalid wallet address:', walletAddress_);
        return res.json({
            userDeposits: 0,
            dsfLpBalance: 0,
            safeRatioUser: 0,
            availableToWithdraw: 0,
            cvxShare: 0,
            cvxCost: 0,
            crvShare: 0,
            crvCost: 0,
            annualYieldRate: 0,
            ethSpent: 0,
            usdSpent: 0,
            ethSaved: 0,
            usdSaved: 0
        });
    }
    
    console.log('\nNormalized Address      :', walletAddress);

    
    if (!/^(0x)?[0-9a-f]{40}$/i.test(walletAddress)) {
        logError("Адрес не соответствует ожидаемому формату.");
    } else {
        logSuccess("Адрес соответствует ожидаемому формату.");
    }
    
    let connection;

    connection = await pool.getConnection();

    try {
        // Проверяем наличие кошелька в базе данных unique_depositors
        const [rows] = await connection.query('SELECT * FROM unique_depositors WHERE depositor_address = ?', [walletAddress]);
        console.log("Rows from database:", rows);
        
        if (rows.length === 0) {
            // Если кошелек не найден в unique_depositors, возвращаем пустые данные
            logWarning('Wallet not found in unique_depositors.');
            return res.json({
                userDeposits: 0,
                dsfLpBalance: 0,
                safeRatioUser: 0,
                availableToWithdraw: 0,
                cvxShare: 0,
                cvxCost: 0,
                crvShare: 0,
                crvCost: 0,
                annualYieldRate: 0,
                ethSpent: 0,
                usdSpent: 0,
                ethSaved: 0,
                usdSaved: 0
            });
        }

        // Проверяем наличие кошелька в базе данных wallet_info
        const [walletRows] = await connection.query('SELECT * FROM wallet_info WHERE wallet_address = ?', [walletAddress]);
        console.log("Rows from database wallet_info:", walletRows);

        if (walletRows.length === 0) {
            // Если кошелек не найден в wallet_info, получаем данные и сохраняем их
            console.log("Получаем данные кошелька");
            try {
                const walletData = await getWalletData(walletAddress);
                
                // Проверяем значения dsfLpBalance и safeRatioUser
                if (walletData.dsfLpBalance > 0 || walletData.safeRatioUser > 0) {

                    const insertQuery = `
                        INSERT INTO wallet_info (
                            wallet_address,
                            user_deposits,
                            dsf_lp_balance,
                            ratio_user,
                            available_to_withdraw,
                            cvx_share,
                            cvx_cost,
                            crv_share,
                            crv_cost,
                            annual_yield_rate,
                            eth_spent,
                            usd_spent,
                            eth_saved,
                            usd_saved,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
                    `;
                    await connection.query(insertQuery, [
                        walletAddress,
                        walletData.userDeposits,
                        walletData.dsfLpBalance,
                        walletData.safeRatioUser,
                        walletData.availableToWithdraw,
                        walletData.cvxShare,
                        walletData.cvxCost,
                        walletData.crvShare,
                        walletData.crvCost,
                        walletData.annualYieldRate,
                        walletData.ethSpent,
                        walletData.usdSpent,
                        walletData.ethSaved,
                        walletData.usdSaved
                    ]);
                    // Отправляем полученные данные клиенту
                    const serializedData = serializeBigints(walletData); // Сериализация данных
                    res.json(serializedData); // Отправка сериализованных данных
                } else {
                    logWarning('Wallet balance or ratio is zero, not saving to DB.');
                    res.json(walletData); // Возвращаем данные без сохранения
                }
            } catch (error) {
                // Логируем ошибку и отправляем ответ сервера
                logError('Failed to retrieve or insert wallet data:', error);
                res.status(500).send('Internal Server Error');
            }
        } else {
            // Если данные уже есть, возвращаем их
            console.log("Данные кошелька уже есть");
            res.json(walletRows[0]);
        }
    } catch (error) {
        // Обработка ошибок при соединении или выполнении SQL-запроса
        logError('Database connection or operation failed:', error);
        res.status(500).send('Internal Server Error');
    } finally {
        // Освобождение соединения
        if (connection) {
            connection.release();
        }
    }
});



// Вызов функции обновления всех кошельков раз в 3 часа
cron.schedule('0 */3 * * *', async () => {
    console.log('Running a task every 3 hours');
    updateAllWallets(); // Вызов функции обновления всех кошельков
});

// NEW Разблокировать после теста
// Создаем cron-задачу для периодического обновления данных APY каждый час
cron.schedule('0 */1 * * *', async () => {
    logInfo('Fetching APY data... Every 1 hours');
    await addNewDayApyData();
}); 

// Создаем cron-задачу для периодического обновления данных APY 
// cron.schedule('* * * * *', async () => {
//     logInfo('Fetching APY data... Every minute');
//     await addNewDayApyData(); 
// });

// NEW
// Создаем cron-задачу для периодического обновления данных APY каждую неделю в понедельник в 00:00
cron.schedule('0 0 * * 1', async () => {
    logInfo('Fetching APY data... Every 1 week');
    await updateApyData();
});



//
//
// APY
//
//

// NEW Apy
// Общая функция для добавления или обновления записи APY
async function upsertApyData(timestamp, apy) {
    const latestDate = timestamp.split('T')[0];
    const latestTime = timestamp.split('T')[1];
    let connection = await pool.getConnection();

    // Получаем все записи с такой же датой
    const checkQuery = `SELECT timestamp FROM apy_info WHERE DATE(timestamp) = DATE(?) ORDER BY timestamp DESC`;
    const [rows] = await connection.query(checkQuery, [timestamp]);

    if (rows.length === 0) {
        // Если записи с такой датой нет, добавляем новую запись
        const insertQuery = `
            INSERT INTO apy_info (timestamp, apy)
            VALUES (?, ?)
            ON DUPLICATE KEY UPDATE
            apy = VALUES(apy), updated_at = CURRENT_TIMESTAMP
        `;
        await connection.query(insertQuery, [timestamp, apy]);
        console.log(`Add - ${timestamp} , APY : ${apy}`);
    } else {
        const existingTimestamp = new Date(rows[0].timestamp).toISOString();
        const existingTime = existingTimestamp.split('T')[1];

        if (latestTime > existingTime) {
            // Если новое время свежее, обновляем запись
            const updateQuery = `
                UPDATE apy_info SET timestamp = ?, apy = ?, updated_at = CURRENT_TIMESTAMP
                WHERE DATE(timestamp) = DATE(?)
            `;
            await connection.query(updateQuery, [timestamp, apy, timestamp]);
            console.log(`Update - ${timestamp} , APY : ${apy}`);
        } else {
            console.log(`${timestamp} - No new data to add or update.`);
        }
    }

    connection.release();
}

// Функция для получения и сохранения всех данных APY
async function updateApyData() {
    console.log("\nStarting update of APY data...\n");
    try {
        const response = await axios.get('https://yields.llama.fi/chart/8a20c472-142c-4442-b724-40f2183c073e');
        const data = response.data.data;

        const startDate = new Date('2022-07-30T00:00:00.000Z');
        const currentDate = new Date();

        for (const entry of data) {
            const entryDate = new Date(entry.timestamp);
            const apy = (entry.apy * 0.85).toFixed(4);

            if (entryDate >= startDate && entryDate <= currentDate) {
                await upsertApyData(entryDate.toISOString(), apy);
            }
        }

        logSuccess("\nAPY data fetched and saved successfully.");
    } catch (error) {
        logError("\nFailed to fetch or save APY data:", error);
    }
}

// Функция для добавления новых данных APY за день
async function addNewDayApyData() {
    try {
        const response = await axios.get('https://yields.llama.fi/chart/8a20c472-142c-4442-b724-40f2183c073e');
        const data = response.data.data;

        if (!data || data.length === 0) {
            logError("No data received from the API.");
            return null;
        }

        const latestEntry = data[data.length - 1];
        const latestTimestamp = new Date(latestEntry.timestamp).toISOString();
        const apy = (latestEntry.apy * 0.85).toFixed(4);

        await upsertApyData(latestTimestamp, apy);

        logSuccess("\nAPY data fetched and saved successfully.");
    } catch (error) {
        logError(`\nFailed to fetch or save APY data: ${error}`);
        return null;
    }
}

// NEW Apy
// Endpoint для получения данных APY
app.get('/apy', async (req, res) => {
    let connection;

    try {
        // Получаем соединение с базой данных
        connection = await pool.getConnection();

        // Получаем все записи APY из базы данных в хронологическом порядке
        const [rows] = await connection.query('SELECT timestamp AS date, apy FROM apy_info ORDER BY timestamp ASC');

        // Отправляем список записей APY клиенту в формате JSON
        res.json(rows);
    } catch (error) {
        // Обработка ошибок при соединении или выполнении SQL-запроса
        logError('Database connection or operation failed:', error);
        res.status(500).send('Internal Server Error');
    } finally {
        // Освобождение соединения
        if (connection) {
            connection.release();
        }
    }
});

// NEW Apy
// Маршрут для вызова обновления данных APY
app.get('/update/apy', async (req, res) => {
    try {
        await updateApyData();
        res.send({ message: 'APY data updated successfully' });
    } catch (error) {
        logError('Failed to update APY data:', error);
        res.status(500).send('Failed to update APY data');
    }
});

// NEW Apy
// Маршрут для вызова обновления только последнего значения APY
app.get('/fetch_latest_apy', async (req, res) => {
    try {
        const latestApyData = await addNewDayApyData();
        if (latestApyData) {
            res.json({ message: 'Latest APY data fetched and saved successfully', data: latestApyData });
        } else {
            res.status(500).send('Failed to fetch latest APY data');
        }
    } catch (error) {
        logError('Failed to fetch latest APY data:', error);
        res.status(500).send('Failed to fetch latest APY data');
    }
});


//
//
// Эвенты
//
//

// NEW
// Функция для преобразования значения в BigInt и деления на десятичное значение
function toBigIntDiv(value, decimals) {
    return (BigInt(value) / BigInt(10 ** decimals)).toString();
}

// NEW
// Инициализация базы данных
async function insertEvent(event) {
    let connection;
    try {
        connection = await pool.getConnection();
        const query = `
            INSERT INTO wallet_events (event, eventDate, transactionCostEth, transactionCostUsd, returnValues, wallet_address)
            VALUES (?, ?, ?, ?, ?, ?)
        `;
        const values = [
            event.event,
            event.eventDate,
            event.transactionCostEth,
            event.transactionCostUsd,
            JSON.stringify(event.returnValues),
            event.wallet_address
        ];
        await connection.query(query, values);
        logSuccess(`Event inserted successfully for wallet address: ${event.wallet_address}`);
    } catch (error) {
        logError(`Failed to insert event for wallet address ${event.wallet_address}:`, error);
    } finally {
        if (connection) connection.release();
    }
}

//
//
// Все эвенты
//
//

// Функция для получения номера блока по дате
async function getBlockNumberByDate(date) {
    const timestamp = Math.floor(date.getTime() / 1000);

    const response = await retryEtherscan(async () => {
        const apiKey = getNextEtherscanApiKey();
        return await axios.get(`https://api.etherscan.io/api`, {
            params: {
                module: 'block',
                action: 'getblocknobytime',
                timestamp: timestamp,
                closest: 'before',
                apikey: apiKey  // Используем переключение API ключей
            }
        });
    });

    if (response.data && response.data.result) {
        return BigInt(response.data.result);
    } else {
        throw new Error('Failed to get block number by date from Etherscan');
    }
}

// Функция для получения исторического курса ETH/USD с Etherscan
async function getEthUsdPrice(date) {
    const maxRetries = 5;
    let retries = 0;

    while (retries < maxRetries) {
        try {

            const response = await retryEtherscan(async () => {
                const apiKey = getNextEtherscanApiKey();
                return await axios.get(`https://api.etherscan.io/api`, {
                    params: {
                        module: 'stats',
                        action: 'ethprice',
                        apikey: apiKey  // Используем переключение API ключей
                    }
                });
            });

            if (response.data && response.data.result) {
                const ethUsdPrice = parseFloat(response.data.result.ethusd);
                if (ethUsdPrice !== 0) {
                    return ethUsdPrice;
                }
            }
        } catch (error) {
            console.error(`Failed to fetch ETH price from Etherscan: ${error.message}`);
        }

        retries += 1;
        console.log(`Retrying to fetch ETH price... (${retries}/${maxRetries})`);
        await new Promise(res => setTimeout(res, 1000)); // Задержка перед повторной попыткой
    }

    throw new Error('Failed to fetch ETH price from Etherscan after multiple retries');
}

let initializationCompleted = false; // Изначально инициализация не завершена
let initializationTelegramBotEvents = false; // Изначально Телеграм бот Уведомляющий об эвентах не запущен

// Функция для начальной инициализации отсутствующих событий
async function initializeMissingEvents() {
    try {
        console.log('Initializing missing events...');

        const lastEventBlockQuery = `SELECT MAX(blockNumber) as lastBlock FROM contract_events`;
        const [rows] = await pool.query(lastEventBlockQuery);
        const lastEventBlock = rows[0].lastBlock ? BigInt(rows[0].lastBlock) : BigInt(0);

        const latestBlock = await fetchLatestBlockFromEtherscan();

        if (lastEventBlock >= latestBlock) {
            console.log(`No new blocks to process.`);
            initializationTelegramBotEvents = true; // Запуск Телеграм бота Уведомляющего об эвентах
            console.log(`telegramBotEvets : activated (${initializationTelegramBotEvents})`);
            return;
        }

        let fromBlock = lastEventBlock + BigInt(1);
        const toBlock = latestBlock;

        const events = await fetchEventsUsingWeb3(fromBlock, toBlock);
        await storeEvents(events);

        console.log(`Fetched and stored missing events up to block ${latestBlock}.`);
        initializationTelegramBotEvents = true; // Запуск Телеграм бота Уведомляющего об эвентах
        console.log(`telegramBotEvets : activated (${initializationTelegramBotEvents})`);
    } catch (error) {
        logError(`Failed to initialize missing events: ${error}`);
    }
}

// Функция для начальной инициализации отсутствующих событий, начиная с (blockNumber - 1) 
// Для поиска пропущеных событий при оптимизированных транзакциях
async function missingCheckForEvernts() {
    try {
        console.log('Additional block check...');

        const lastEventBlockQuery = `SELECT MAX(blockNumber) as lastBlock FROM contract_events`;
        const [rows] = await pool.query(lastEventBlockQuery);
        const lastEventBlock = rows[0].lastBlock ? BigInt(rows[0].lastBlock) : BigInt(0);

        const latestBlock = await fetchLatestBlockFromEtherscan();

        if (lastEventBlock >= latestBlock) {
            console.log(`No new blocks to process.`);
            return;
        }

        // Начинаем с предыдущего блока
        let fromBlock = lastEventBlock - BigInt(1);
        if (fromBlock < 0) {
            fromBlock = BigInt(0); // Убедитесь, что fromBlock не меньше 0
        }
        const toBlock = latestBlock;

        const events = await fetchEventsUsingWeb3(fromBlock, toBlock);
        await storeEvents(events);

        //console.log(`Fetched and stored missing events up to block ${latestBlock}.`);
    } catch (error) {
        logError(`Failed to initialize missing events: ${error}`);
    }
}

//New2
// Функция для записи последнего проверенного блока в базу данных
async function updateLastCheckedBlock(blockNumber) {
    try {
        // const updateQuery = `UPDATE settings SET setting_value = ? WHERE setting_key = 'last_checked_block'`;
        // await pool.query(updateQuery, [blockNumber.toString()]);
        //console.log(`Updated last checked block to ${blockNumber}`);
        const updateQuery = `INSERT INTO settings (setting_key, value) VALUES ('last_checked_block', ?) ON DUPLICATE KEY UPDATE value = VALUES(value)`;
        await pool.query(updateQuery, [blockNumber.toString()]);
    } catch (error) {
        console.error(`Failed to update last checked block: ${error.message}`);
    }
}
//New2
// Функция для получения последнего проверенного блока из базы данных
async function getLastCheckedBlock() {
    const selectQuery = `SELECT value FROM settings WHERE setting_key = 'last_checked_block'`;
    const [rows] = await pool.query(selectQuery);
    return rows.length ? BigInt(rows[0].value) : BigInt(0);
}

// 1 //New2
// Функция для проверки наличия новых событий
async function checkForNewEvents() {
    if (!initializationCompleted) {
        //console.log('Initialization not completed. Skipping check for new events.');
        return;
    }

    initializationCompleted = false;

    try {
        logWarning('Checking for new events...');

        // Получение номера последнего блока с событиями из базы данных
        const lastEventBlockQuery = `SELECT MAX(blockNumber) as lastBlock FROM contract_events`;
        const [rows] = await pool.query(lastEventBlockQuery);
        const lastEventBlock = rows[0].lastBlock ? BigInt(rows[0].lastBlock) : BigInt(0);
        //console.log(`Last event block from database: ${lastEventBlock}`);

        // Получаем номер последнего блока из Etherscan
        const latestBlock = BigInt(await fetchLatestBlockFromEtherscan());
        //console.log(`Latest block from Etherscan: ${latestBlock}`);
        
        // Получение номера последнего проверенного блока из таблицы settings
        const lastCheckedBlock = BigInt(await getLastCheckedBlock());
        //console.log(`Last checked block from settings: ${lastCheckedBlock}`);

        // Общее логирование информации о блоках
        console.log(`Block Info - Last event block from database: ${lastEventBlock}, Latest block from Etherscan: ${latestBlock}, Last checked block from settings: ${lastCheckedBlock}`);

        // Выбор начального блока для проверки
        let fromBlock;
        if (lastCheckedBlock === BigInt(0)) {
            fromBlock = lastEventBlock + BigInt(1);
        } else if (lastEventBlock >= lastCheckedBlock) {
            fromBlock = lastEventBlock + BigInt(1);
        } else {
            fromBlock = lastCheckedBlock + BigInt(1);
        }

        if (fromBlock > latestBlock) {
            console.log('No new blocks to process.');
            initializationCompleted = true;
            return;
        }
        
        let newEventsFetched = false;
        let autoCompoundAllFound = false; // Флаг для проверки события AutoCompoundAll
        
        // Проверка новых блоков на события
        while (fromBlock <= latestBlock) {
            const toBlock = fromBlock + BigInt(1000) <= latestBlock ? fromBlock + BigInt(9999) : latestBlock;

            process.stdout.write(`Checking blocks from ${fromBlock} to ${toBlock} - `);

            // Получение событий в диапазоне блоков
            const events = await fetchEventsWithRetry(fromBlock, toBlock);

            if (events.length > 0) {
                await storeEvents(events, true); // Сохраняем новые события в базе данных. Передаем true, чтобы указывать, что это новые события 
                newEventsFetched = true; // Устанавливаем флаг, если есть новые события
                console.log(`Stored ${events.length} events from blocks ${fromBlock} to ${toBlock}`);
            
                // Проверка на наличие события AutoCompoundAll
                if (events.some(event => event.event === 'AutoCompoundAll')) {
                    autoCompoundAllFound = true;
                }
            }

            // Проверка блока на наличие других эвентов, если есть 'Deposited' или 'Withdrawn'
            for (const event of events) {
                if (['Deposited', 'Withdrawn'].includes(event.event)) {
                    const blockNumber = event.blockNumber;
                    const blockEvents = await fetchEventsUsingWeb3(blockNumber, blockNumber);
                    await storeEvents(blockEvents, true); // Сохранение дополнительных событий
                    newEventsFetched = true; // Устанавливаем флаг, если есть новые события
                    console.log(`Additional events found in block ${blockNumber} and stored`);
                
                    // Проверка на наличие события AutoCompoundAll среди дополнительных событий
                    if (blockEvents.some(event => event.event === 'AutoCompoundAll')) {
                        autoCompoundAllFound = true;
                    }
                }
            }
            
            fromBlock = toBlock + BigInt(1);
        }
    
        if (newEventsFetched) {
            logSuccess('New events found, updating unique depositors and wallets.');
            
            //await missingCheckForEvernts() // проверяем блок на упущенные события

            await populateUniqueDepositors(); // Обновляем таблицу уникальных депозиторов
            await updateAllWallets(); // Обновляем все кошельки
        }

        //process.stdout.write(`Fetched and stored new events up to block ${latestBlock}.`);
        await updateLastCheckedBlock(latestBlock); // Обновляем последний проверенный блок на latestBlock
        console.log(`Fetched and stored new events up to block & Last checked block updated to ${latestBlock}`);

        // Если было найдено событие AutoCompoundAll, запускаем calculateIncomeDSF
        if (autoCompoundAllFound) {
            logWarning('AutoCompoundAll event found, calculating incomeDSF...');
            await calculateIncomeDSF();
        }

    } catch (error) {
        if (error.code === 'PROTOCOL_CONNECTION_LOST') {
            logError('Connection lost. Reconnecting...');
            await createPool(); // Создаем новый пул соединений
        }
        logError(`Failed to check for new events: ${error}`);
    } finally {
        initializationCompleted = true;
    }
}

/// 1
// Функция для получения событий из Etherscan
async function fetchLatestBlockFromEtherscan() {
    try {
        const response = await retryEtherscan(async () => {
            const apiKey = getNextEtherscanApiKey();
            return await axios.get(`https://api.etherscan.io/api`, {
                params: {
                    module: 'proxy',
                    action: 'eth_blockNumber',
                    apikey: apiKey  // Используем переключение API ключей
                }
            });
        });

        if (response.data && response.data.result) {
            return BigInt(response.data.result);
        } else {
            console.error('Failed to fetch latest block number from Etherscan:', response.data);
            throw new Error('Failed to fetch latest block number from Etherscan');
        }
    } catch (error) {
        logError(`An error occurred while fetching the latest block number: ${error}`);
        throw error;
    }
}

// Функция для получения событий с повторными попытками
async function fetchEventsWithRetry(fromBlock, toBlock, retries = 3) {
    let attempt = 0;
    while (attempt < retries) {
        try {
            return await fetchEventsUsingWeb3(fromBlock, toBlock);
        } catch (error) {
            if (error.innerError && error.innerError.code === 429) {
                console.log(`Rate limit exceeded, retrying in ${attempt + 1} seconds...`);
                await new Promise(resolve => setTimeout(resolve, (attempt + 1) * 1000));
                attempt++;
            } else {
                logError(`Error fetching events from ${fromBlock} to ${toBlock}: ${error.message}`);
                throw error;
            }
        }
    }
    throw new Error(`Failed to fetch events from ${fromBlock} to ${toBlock} after ${retries} attempts`);
}

// 1
//Получение и запись событий через web3
async function fetchEventsUsingWeb3(fromBlock, toBlock) {
    const eventNames = [
        'CreatedPendingDeposit',
        'CreatedPendingWithdrawal',
        'Deposited',
        'Withdrawn',
        'Transfer',
        'FailedDeposit',
        'FailedWithdrawal',
        'AutoCompoundAll',
        'ClaimedAllManagementFee'
    ];

    try {
        // console.log(`Fetching events from block ${fromBlock} to ${toBlock}`);
        // console.log(`Event names: ${eventNames.join(', ')}`);

        const eventsPromises = eventNames.map(async eventName => {
            const events = await contractDSF.getPastEvents(eventName, {
                fromBlock: fromBlock.toString(),
                toBlock: toBlock.toString()
            });
            //await new Promise(resolve => setTimeout(resolve, 1000)); // Задержка 1 секунда между запросами для разных событий
            return events;
        });

        const allEvents = await Promise.all(eventsPromises);
        const flatEvents = allEvents.flat();
        console.log(`Fetched ${flatEvents.length} events`);
        
        return flatEvents;
    } catch (error) {
        if (error.message.includes('rate limit')) {
            console.log('Rate limit exceeded, waiting before retrying...');
            await new Promise(res => setTimeout(res, 20000)); // Ждем 20 секунд перед повторной попыткой
            return fetchEventsUsingWeb3(fromBlock, toBlock);
        } else {
            console.error(`Error fetching events from block ${fromBlock} to ${toBlock}: ${error.message}`);
            throw error;
        }
    }
}

function convertBigIntToString(obj) {
    if (typeof obj === 'bigint') {
        return obj.toString();
    } else if (Array.isArray(obj)) {
        return obj.map(convertBigIntToString);
    } else if (typeof obj === 'object' && obj !== null) {
        return Object.fromEntries(Object.entries(obj).map(([k, v]) => [k, convertBigIntToString(v)]));
    } else {
        return obj;
    }
}

// тест
async function calculateAvailableToWithdraw(valueOrShares) {
    const totalSupply_ = await contractDSF.methods.totalSupply().call();
    const ratioUser_ = valueOrShares / totalSupply_;
    console.log("\nvalueOrShares", valueOrShares ,"\nratioUser_ = ", ratioUser_)
    availableToWithdraw_ = await contractDSFStrategy.methods.calcWithdrawOneCoin(ratioUser_, 2).call();
    return parseFloat(availableToWithdraw_.toString());
}

// Функция для форматирования больших чисел
function formatBigInt(value, decimals) {
    return (Number(BigInt(value)) / Math.pow(10, decimals)).toFixed(decimals);
}

// тест
async function storeEvents(events, isNewEvents = false) {

    // Separate Transfer events from other events
    const transferEvents = [];
    const otherEvents = [];

    for (const event of events) {
        if (event.event === 'Transfer') {
            transferEvents.push(event);
        } else {
            otherEvents.push(event);
        }
    }

    // Process non-Transfer events first
    // Сначала обрабатываем события, отличные от Transfer
    for (const event of otherEvents) {
        await processEvent(event, isNewEvents);
    }

    // Process Transfer events
    // Затем обрабатываем события Transfer
    for (const event of transferEvents) {
        await processEvent(event, isNewEvents);
    }
}

// тест
async function processEvent(event, isNewEvents) {
    const block = await web3.eth.getBlock(event.blockNumber);
    const eventDate = new Date(Number(block.timestamp) * 1000);
    const transaction = await web3.eth.getTransaction(event.transactionHash);
    const receipt = await web3.eth.getTransactionReceipt(event.transactionHash);
    const gasUsed = BigInt(receipt.gasUsed);
    const gasPrice = BigInt(transaction.gasPrice);
    const transactionCostEth = web3.utils.fromWei((gasUsed * gasPrice).toString(), 'ether');
    const ethUsdPrice = await getEthUsdPrice(Number(block.timestamp));
    const transactionCostUsd = (parseFloat(transactionCostEth) * ethUsdPrice).toFixed(2);

    // Пропускаем события 'Transfer', которые не соответствуют условиям
    if (event.event === 'Transfer' &&
        (event.returnValues.from === '0x0000000000000000000000000000000000000000' ||
        event.returnValues.to === '0x0000000000000000000000000000000000000000' ||
        event.returnValues.from.toLowerCase() === contractDSF.options.address.toLowerCase() ||
        event.returnValues.to.toLowerCase() === contractDSF.options.address.toLowerCase())) {
        return;
    }

    let formattedEvent = {
        event: event.event,
        eventDate: eventDate.toISOString(),
        transactionCostEth: transactionCostEth,
        transactionCostUsd: transactionCostUsd,
        blockNumber: event.blockNumber,
        transactionHash: event.transactionHash,
        returnValues: {}
    };

    const blockNumberStr = event.blockNumber.toString();

    switch (event.event) {
        case 'CreatedPendingDeposit':
            formattedEvent.returnValues = {
                depositor: event.returnValues.depositor,
                amounts: {
                    DAI: formatBigInt(event.returnValues.amounts[0], 18),
                    USDC: formatBigInt(event.returnValues.amounts[1], 6),
                    USDT: formatBigInt(event.returnValues.amounts[2], 6)
                }
            };

            if (initializationTelegramBotEvents) {
                const message = `Event 'CreatedPendingDeposit' detected!
                    \nURL    : https://etherscan.io/tx/${event.transactionHash}
                    \nWallet : ${event.returnValues.depositor}
                    \nDAI    : ${formatBigInt(event.returnValues.amounts[0], 18)}
                    \nUSDC   : ${formatBigInt(event.returnValues.amounts[1], 6)}
                    \nUSDT   : ${formatBigInt(event.returnValues.amounts[2], 6)}`;
                console.log(message);
                sendMessageToChat(message);
            };
        
            break;
        case 'CreatedPendingWithdrawal':
            formattedEvent.returnValues = {
                withdrawer: event.returnValues.withdrawer,
                lpShares: formatBigInt(event.returnValues.lpShares, 18),
                tokenAmounts: {
                    DAI: formatBigInt(event.returnValues.tokenAmounts[0], 18),
                    USDC: formatBigInt(event.returnValues.tokenAmounts[1], 6),
                    USDT: formatBigInt(event.returnValues.tokenAmounts[2], 6)
                }
            };

            if (initializationTelegramBotEvents) {
                const message = `Event 'CreatedPendingWithdrawal' detected!
                    \nURL    : https://etherscan.io/tx/${event.transactionHash}
                    \nWallet : ${event.returnValues.withdrawer}
                    \nDSF LP : ${formatBigInt(event.returnValues.lpShares, 18)}
                    \nDAI    : ${formatBigInt(event.returnValues.tokenAmounts[0], 18)}
                    \nUSDC   : ${formatBigInt(event.returnValues.tokenAmounts[1], 6)}
                    \nUSDT   : ${formatBigInt(event.returnValues.tokenAmounts[2], 6)}`;
                console.log(message);
                sendMessageToChat(message);
            };

            break;
        case 'Deposited': 
            const transactionsD_status = await showBlockTransactions(contractsLib.DSFwallet, blockNumberStr);     
            logError(transactionsD_status)
            formattedEvent.returnValues = {
                depositor: event.returnValues.depositor,
                amounts: {
                    DAI: formatBigInt(event.returnValues.amounts[0], 18),
                    USDC: formatBigInt(event.returnValues.amounts[1], 6),
                    USDT: formatBigInt(event.returnValues.amounts[2], 6)
                },
                lpShares: formatBigInt(event.returnValues.lpShares, 18),
                transaction_status: transactionsD_status
            };

            if (initializationTelegramBotEvents) {
                const message = `Event 'Deposited' ${transactionsD_status} detected!
                    \nURL    : https://etherscan.io/tx/${event.transactionHash}
                    \nWallet : ${event.returnValues.depositor}
                    \nDSF LP : ${formatBigInt(event.returnValues.lpShares, 18)}
                    \nDAI    : ${formatBigInt(event.returnValues.amounts[0], 18)}
                    \nUSDC   : ${formatBigInt(event.returnValues.amounts[1], 6)}
                    \nUSDT   : ${formatBigInt(event.returnValues.amounts[2], 6)}`;
                console.log(message);
                sendMessageToChat(message);
            };

            break;
        case 'Withdrawn':
                const transactionsW_status = await showBlockTransactions(contractsLib.DSFwallet, blockNumberStr);
                logError(transactionsW_status)
                const realWithdrawnDAI = await getDAITransactionsByBlock(event.returnValues.withdrawer, blockNumberStr);
                const realWithdrawnUSDC = await getUSDCTransactionsByBlock(event.returnValues.withdrawer, blockNumberStr);
                const realWithdrawnUSDT = await getUSDTTransactionsByBlock(event.returnValues.withdrawer, blockNumberStr);
                formattedEvent.returnValues = {
                    withdrawer: event.returnValues.withdrawer,
                    withdrawalType: event.returnValues.withdrawalType.toString(),
                    tokenAmounts: {
                        DAI: formatBigInt(event.returnValues.tokenAmounts[0], 18),
                        USDC: formatBigInt(event.returnValues.tokenAmounts[1], 6),
                        USDT: formatBigInt(event.returnValues.tokenAmounts[2], 6)
                    },
                    lpShares: formatBigInt(event.returnValues.lpShares, 18),
                    tokenIndex: event.returnValues.tokenIndex.toString(),
                    transaction_status: transactionsW_status,
                    realWithdrawnAmount: {
                        DAI: realWithdrawnDAI,
                        USDC: realWithdrawnUSDC,
                        USDT: realWithdrawnUSDT
                    }
                };

                if (initializationTelegramBotEvents) {
                    const message = `Event 'Withdrawn' ${transactionsW_status} detected!
                        \nURL    : https://etherscan.io/tx/${event.transactionHash}
                        \nWallet : ${event.returnValues.withdrawer}
                        \nDSF LP : ${formatBigInt(event.returnValues.lpShares, 18)}
                        \nDAI    : ${realWithdrawnDAI}
                        \nUSDC   : ${realWithdrawnUSDC}
                        \nUSDT   : ${realWithdrawnUSDT}`;
                    console.log(message);
                    sendMessageToChat(message);
                };

                break;
        case 'FailedDeposit':
            const transactionsFD_status = await showBlockTransactions(contractsLib.DSFwallet, blockNumberStr);     
            logError(transactionsFD_status)
            formattedEvent.returnValues = {
                depositor: event.returnValues.depositor,
                amounts: {
                    DAI: formatBigInt(event.returnValues.amounts[0], 18),
                    USDC: formatBigInt(event.returnValues.amounts[1], 6),
                    USDT: formatBigInt(event.returnValues.amounts[2], 6)
                },
                lpShares: formatBigInt(event.returnValues.lpShares, 18),
                transaction_status: transactionsFD_status
            };

            if (initializationTelegramBotEvents) {
                const message = `Event 'FailedDeposit' ${transactionsFD_status} detected!
                    \nURL    : https://etherscan.io/tx/${event.transactionHash}
                    \nWallet : ${event.returnValues.depositor}
                    \nDSF LP : ${formatBigInt(event.returnValues.lpShares, 18)}
                    \nDAI    : ${formatBigInt(event.returnValues.amounts[0], 18)}
                    \nUSDC   : ${formatBigInt(event.returnValues.amounts[1], 6)}
                    \nUSDT   : ${formatBigInt(event.returnValues.amounts[2], 6)}`;
                console.log(message);
                sendMessageToChat(message);
            };

            break;
        case 'FailedWithdrawal':
            const transactionsFW_status = await showBlockTransactions(contractsLib.DSFwallet, blockNumberStr);     
            logError(transactionsFW_status)
            formattedEvent.returnValues = {
                withdrawer: event.returnValues.withdrawer,
                amounts: {
                    DAI: formatBigInt(event.returnValues.amounts[0], 18),
                    USDC: formatBigInt(event.returnValues.amounts[1], 6),
                    USDT: formatBigInt(event.returnValues.amounts[2], 6)
                },
                lpShares: formatBigInt(event.returnValues.lpShares, 18),
                transaction_status: transactionsFW_status
            };

            if (initializationTelegramBotEvents) {
                const message = `Event 'FailedWithdrawal' ${transactionsFW_status} detected!
                    \nURL    : https://etherscan.io/tx/${event.transactionHash}
                    \nWallet : ${event.returnValues.withdrawer}
                    \nDSF LP : ${formatBigInt(event.returnValues.lpShares, 18)}
                    \nDAI    : ${formatBigInt(event.returnValues.amounts[0], 18)}
                    \nUSDC   : ${formatBigInt(event.returnValues.amounts[1], 6)}
                    \nUSDT   : ${formatBigInt(event.returnValues.amounts[2], 6)}`;
                console.log(message);
                sendMessageToChat(message);
            };

            break;
        case 'AutoCompoundAll':
                logWarning(`addressesStrategy : ${addressesStrategy}`);
                // Проверка, что addressesStrategy определен и является массивом
                if (!Array.isArray(addressesStrategy) || addressesStrategy.length === 0) {
                    logError(`addressesStrategy должен быть массивом и не должен быть пустым.`);
                    throw new Error('addressesStrategy должен быть массивом и не должен быть пустым.');
                }
    
                // Преобразование blockNumber в строку, если это число
                const balanceDifference = await getBalanceDifferencesForAddresses(addressesStrategy, blockNumberStr);
                formattedEvent.returnValues = {
                    incomeDSF: Number(balanceDifference)
                };

                if (initializationTelegramBotEvents) {
                    const message = `Event 'AutoCompoundAll' detected!
                        \nURL    : https://etherscan.io/tx/${event.transactionHash}
                        \nIncome : ${Number(balanceDifference)}`;
                    console.log(message);
                    sendMessageToChat(message);
                };

                break;
        case 'ClaimedAllManagementFee':
                formattedEvent.returnValues = {
                    feeValue: formatBigInt(event.returnValues.feeValue, 6)
                };

                if (initializationTelegramBotEvents) {
                    const message = `Event 'ClaimedAllManagementFee' detected!
                        \nURL    : https://etherscan.io/tx/${event.transactionHash}
                        \nFee    : ${formatBigInt(event.returnValues.feeValue, 6)}`;
                    console.log(message);
                    sendMessageToChat(message);
                };

                break;
        case 'Transfer':
            if (
                event.returnValues.from !== '0x0000000000000000000000000000000000000000' &&
                event.returnValues.to !== '0x0000000000000000000000000000000000000000' &&
                event.returnValues.from.toLowerCase() !== contractDSF.options.address.toLowerCase() &&
                event.returnValues.to.toLowerCase() !== contractDSF.options.address.toLowerCase()
            ) {
                // Проверка на наличие записи в availableToWithdraw для Transfer
                const [withdrawRecords] = await pool.query(
                    `SELECT availableToWithdraw FROM availableToWithdraw 
                    WHERE transactionHash = ? AND event = 'Transfer'`,
                    [event.transactionHash]
                );

                let usdValue;
                if (withdrawRecords.length > 0) {
                    console.log(`Found availableToWithdraw record for Transfer event: ${event.transactionHash}`);
                    usdValue = parseFloat(withdrawRecords[0].availableToWithdraw).toFixed(2);
                } else {
                    usdValue = await calculateTransferUSDValue(event);
                }
                
                formattedEvent.returnValues = {
                    from: event.returnValues.from,
                    to: event.returnValues.to,
                    value: formatBigInt(event.returnValues.value, 18),
                    usdValue: usdValue
                };

                if (initializationTelegramBotEvents) {
                    const message = `Event 'Transfer' detected!
                    \nURL    : https://etherscan.io/tx/${event.transactionHash}
                    \nFrom   : ${event.returnValues.from}
                    \nTo     : ${event.returnValues.to}
                    \nDSF LP : ${formatBigInt(event.returnValues.value, 18)}`;
                    console.log(message);
                    sendMessageToChat(message);
                };

            }
            break;
    }

    // Проверяем, есть ли событие уже в базе данных
    const [existingEvent] = await pool.query(
        `SELECT COUNT(*) as count 
         FROM contract_events 
         WHERE event = ? AND returnValues = ? AND transactionCostEth = ? AND blockNumber = ?`,
        [formattedEvent.event, JSON.stringify(formattedEvent.returnValues), formattedEvent.transactionCostEth, formattedEvent.blockNumber]
    );

    if (existingEvent[0].count === 0) {
        await pool.query(
            `INSERT INTO contract_events (transactionHash, blockNumber, event, eventDate, transactionCostEth, transactionCostUsd, returnValues)
             VALUES (?, ?, ?, ?, ?, ?, ?)`,
            [
                formattedEvent.transactionHash,
                formattedEvent.blockNumber,
                formattedEvent.event,
                formattedEvent.eventDate,
                formattedEvent.transactionCostEth,
                formattedEvent.transactionCostUsd,
                JSON.stringify(formattedEvent.returnValues)
            ]
        );
        console.log(`Stored event: ${formattedEvent.event} - ${formattedEvent.transactionHash}`);
    } else {
        console.log(`Event already exists: ${formattedEvent.transactionHash}`);
    }

    // Если событие Transfer или Deposited или Withdrawn, и это новое событие, рассчитываем и записываем availableToWithdraw
    if (isNewEvents && (formattedEvent.event === 'Transfer' || formattedEvent.event === 'Deposited' || formattedEvent.event === 'Withdrawn')) {
        await storeAvailableToWithdraw(formattedEvent);
    }
}

// считаем withdrawOneCoin для событие Transfer, Deposited, Withdrawn
async function storeAvailableToWithdraw(event) {
    let valueOrShares;

    if (event.event === 'Transfer') {
        valueOrShares = event.returnValues.value;
    } else if (event.event === 'Deposited') {
        valueOrShares = event.returnValues.lpShares; 
    } else if (event.event === 'Withdrawn') {
        valueOrShares = event.returnValues.lpShares;
    }

    const availableToWithdraw_ = await calculateAvailableToWithdraw(valueOrShares);

    await pool.query(
        `INSERT INTO availableToWithdraw (event, eventDate, blockNumber, transactionHash, availableToWithdraw)
         VALUES (?, ?, ?, ?, ?)`,
        [event.event, event.eventDate, event.blockNumber, event.transactionHash, availableToWithdraw_]
    );

    console.log(`Stored availableToWithdraw for event: ${event.event} - ${event.transactionHash}`);
}

// Функция для расчета эквивалента value в USD для события Transfer
async function calculateTransferUSDValue(event) {
    console.log(`Calculating USD value for Transfer event: ${event.transactionHash}`);
    const address = event.returnValues.from;
    //const value = BigInt(event.returnValues.value);
    let balanceUSD = 0;
    let lpShares = 0;


    // Проверка на наличие записи в availableToWithdraw
    const [withdrawRecords] = await pool.query(
        `SELECT availableToWithdraw FROM availableToWithdraw 
         WHERE transactionHash = ? AND event = 'Transfer'`,
        [event.transactionHash]
    );

    if (withdrawRecords.length > 0) {
        console.log(`Found availableToWithdraw record for event: ${event.transactionHash}`);
        return parseFloat(withdrawRecords[0].availableToWithdraw).toFixed(2);
    }

    const [events] = await pool.query(
        `SELECT * FROM contract_events 
         WHERE JSON_EXTRACT(returnValues, '$.depositor') = ? OR JSON_EXTRACT(returnValues, '$.withdrawer') = ? 
         OR JSON_EXTRACT(returnValues, '$.from') = ? OR JSON_EXTRACT(returnValues, '$.to') = ? 
         ORDER BY eventDate ASC`,
        [address, address, address, address]
    );

    for (const event of events) {
        console.log(`Processing event: ${event.event} - ${event.transactionHash}`);
        let returnValues = event.returnValues;
        if (typeof returnValues === 'string') {
            try {
                returnValues = JSON.parse(returnValues);
            } catch (error) {
                console.error(`Failed to parse returnValues for event: ${event.transactionHash}`, error);
                continue;
            }
        }

        if (event.event === 'Deposited' && returnValues.depositor === address) {
           // Проверка на наличие записи в availableToWithdraw для Deposited
           const [depositRecords] = await pool.query(
                `SELECT availableToWithdraw FROM availableToWithdraw 
                WHERE transactionHash = ? AND event = 'Deposited'`,
                [event.transactionHash]
            );

            if (depositRecords.length > 0) {
                console.log(`Found availableToWithdraw record for Deposited event: ${event.transactionHash}`);
                balanceUSD = parseFloat(depositRecords[0].availableToWithdraw);
            } else {
                const depositedUSD = parseFloat(returnValues.amounts.DAI) + parseFloat(returnValues.amounts.USDC) + parseFloat(returnValues.amounts.USDT);
                console.log(`Deposited ${depositedUSD} , Deposited - 0.0016 = ${depositedUSD - (depositedUSD * 0.0016)}`);
                balanceUSD += depositedUSD - (depositedUSD * 0.0016);
            }
            lpShares += parseFloat(returnValues.lpShares);
        } else if (event.event === 'Withdrawn' && returnValues.withdrawer === address) {
            const withdrawnLpShares = parseFloat(returnValues.lpShares);
            const sharePercentage = withdrawnLpShares / lpShares;
            const withdrawnUSD = balanceUSD * sharePercentage;
            balanceUSD -= withdrawnUSD;
            lpShares -= withdrawnLpShares;
        } else if (event.event === 'Transfer') {
            if (returnValues.from === address) {
                const transferLpShares = parseFloat(returnValues.value);
                const transferUSD = (balanceUSD / lpShares) * transferLpShares;
                balanceUSD -= transferUSD;
                lpShares -= transferLpShares;
            } else if (returnValues.to === address) {
                const transferLpShares = parseFloat(returnValues.value);
                const transferUSD = (balanceUSD / lpShares) * transferLpShares;
                balanceUSD += transferUSD;
                lpShares += transferLpShares;
            }
        }
    }

    if (lpShares === 0) {
        console.log(`No lpShares available for address: ${address}`);
        return 0;
    }
    
    const usdValue = (balanceUSD / lpShares) * parseFloat(event.returnValues.value) / Math.pow(10, 18); 
    console.log(`Calculated USD value for Transfer event: ${event.transactionHash} is ${usdValue}`);
    return usdValue.toFixed(2);
}

// Новый маршрут для получения эвента по хэшу транзакции
app.get('/events/hash/:transactionHash', async (req, res) => {
    const transactionHash = req.params.transactionHash;

    logSuccess(`\nInfo about event, Transaction Hash : ${transactionHash}`);

    try {
        // Найдите событие Transfer по его хэшу транзакции
        const [events] = await pool.query(
            `SELECT * FROM contract_events WHERE transactionHash = ? AND event = 'Transfer'`,
            [transactionHash]
        );

        console.log('\n', events);

        if (events.length === 0) {
            return res.status(404).send('Transfer event not found');
        }

        const event = events[0];

        res.json({ event });
    } catch (error) {
        console.error(`\nFailed to calculate USD value for Transfer event: ${transactionHash}`, error);
        res.status(500).send('\nFailed to calculate USD value for Transfer event');
    }
});
    
// Новый маршрут для получения всех событий
app.get('/events', async (req, res) => {
    try {
        const [events] = await pool.query('SELECT * FROM contract_events ORDER BY eventDate DESC');
        res.json(events);
    } catch (error) {
        console.error('Failed to fetch contract events:', error);
        res.status(500).send('Failed to fetch contract events');
    }
});

// Эндпоинт для получения событий, связанных с конкретным кошельком
app.get('/events/:wallet', async (req, res) => {
    const { wallet } = req.params;
    
    try {
        const query = `
            SELECT * FROM contract_events 
            WHERE JSON_EXTRACT(returnValues, '$.depositor') = ?
            OR JSON_EXTRACT(returnValues, '$.withdrawer') = ?
            OR JSON_EXTRACT(returnValues, '$.to') = ?
            OR JSON_EXTRACT(returnValues, '$.from') = ?
            ORDER BY blockNumber ASC
        `;
        
        const [rows] = await pool.query(query, [wallet, wallet, wallet, wallet]);
        res.json(rows);
    } catch (error) {
        console.error(`Failed to fetch events for wallet ${wallet}: ${error.message}`);
        res.status(500).send('Failed to fetch events');
    }
});

//
//
// Заполняем таблицу адресов 
//
//

// Функция для извлечения уникальных адресов депозиторов из таблицы contract_events, из событий 'Deposited', CreatedPendingDeposit и 'Transfer'
async function extractUniqueDepositors() {
    let connection;
    try {
        connection = await pool.getConnection();
        const [rows] = await connection.query(`
            SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.depositor')) AS depositor
            FROM contract_events
            WHERE event = 'Deposited'
            UNION
            SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.to')) AS depositor
            FROM contract_events
            WHERE event = 'Transfer'
            UNION
            SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.depositor')) AS depositor
            FROM contract_events
            WHERE event = 'CreatedPendingDeposit'
        `);
        return rows.map(row => row.depositor);
    } catch (error) {
        console.error('Failed to extract unique depositors:', error);
        return [];
    } finally {
        if (connection) connection.release();
    }
}

// Функция для заполнения таблицы unique_depositors уникальными адресами
async function populateUniqueDepositors() {
    try {
        console.log('Populating unique depositors...');
        const uniqueDepositors = await extractUniqueDepositors();
        let addedCount = 0;
        let existingCount = 0;

        for (const depositor of uniqueDepositors) {
            try {
                const [result] = await pool.query(
                    `INSERT INTO unique_depositors (depositor_address)
                     VALUES (?)
                     ON DUPLICATE KEY UPDATE depositor_address = depositor_address`,
                    [depositor]
                );
                
                // Если затронутая строка - это добавление нового адреса
                if (result.affectedRows > 0 && result.warningCount === 0) {
                    addedCount++;
                    console.log(`Add new          : ${depositor}`);
                } else {
                    existingCount++;
                    console.log(`Already existing : ${depositor}`);
                }
            } catch (error) {
                console.error(`Failed to add depositor ${depositor}:`, error);
            }
        }
        console.log(`\nFinished populating unique depositors.`);
        console.log(`Total unique depositors : ${existingCount+addedCount}`);
        console.log(`Added new               : ${addedCount}`);
        console.log(`Already existing        : ${existingCount}\n`);
    } catch (error) {
        console.error('Failed to populate unique depositors:', error);
    }
}

// Маршрут для получения всех уникальных адресов
app.get('/depositors', async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM unique_depositors ORDER BY id DESC');
        res.json(rows);
    } catch (error) {
        console.error('Failed to fetch unique depositors:', error);
        res.status(500).send('Failed to fetch unique depositors');
    }
});

//
//
//  Персональные ставки доходности
//
//

// Функция для расчета персональной ставки доходности
async function calculatePersonalYieldRate() {
    let connection;
    try {
        connection = await pool.getConnection();

        // Получаем всех депозиторов из таблицы unique_depositors
        const [depositors] = await connection.query('SELECT depositor_address FROM unique_depositors');
        logInfo(`Found ${depositors.length} depositors.`);

        for (const depositor of depositors) {
            const walletAddress = depositor.depositor_address;
            logInfo(`Processing depositor: ${walletAddress}`);

            // Получаем все события для данного депозитора
            const [events] = await connection.query(
                `SELECT * FROM contract_events 
                 WHERE JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.from')) = ? 
                    OR JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.to')) = ? 
                    OR JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.depositor')) = ? 
                    OR JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$.withdrawer')) = ? 
                 ORDER BY eventDate ASC`,
                [walletAddress, walletAddress, walletAddress, walletAddress]
            );

            if (events.length === 0) {
                logInfo(`No events found for depositor: ${walletAddress}`);
                continue;
            }

            let totalDepositedUSD = 0;
            let totalLpShares = 0;
            let totalIncome = 0;
            let previousDate = null;

            for (const event of events) {
                let returnValues = event.returnValues;

                // Проверка, если returnValues - строка, тогда парсинг
                if (typeof returnValues === 'string') {
                    try {
                        returnValues = JSON.parse(returnValues);
                    } catch (error) {
                        console.error(`Failed to parse returnValues for event: ${event.transactionHash}`, error);
                        continue;
                    }
                }

                const eventDate = new Date(event.eventDate);
                const eventDateOnly = eventDate.toISOString().split('T')[0];

                // Если дата события изменилась, рассчитываем доход и записываем данные за предыдущий день
                if (previousDate && previousDate !== eventDateOnly) {
                    // Получаем APY для текущей даты события
                    const [apyRecords] = await connection.query(
                        `SELECT apy FROM apy_info 
                         WHERE DATE(timestamp) = DATE(?)`,
                        [previousDate]
                    );

                    if (apyRecords.length > 0) {
                        const dailyYieldRate = parseFloat(apyRecords[0].apy) / 365 / 100;
                        const dailyIncome = totalDepositedUSD * dailyYieldRate;
                        totalIncome += dailyIncome;

                        const annualApy = (Math.pow((1 + dailyYieldRate), 365) - 1) * 100;

                        // Вставляем или обновляем запись для текущего депозитора и даты
                        const insertQuery = `
                            INSERT INTO personal_yield_rate (depositor_address, date, daily_income, daily_yield_rate, annual_apy)
                            VALUES (?, ?, ?, ?, ?)
                            ON DUPLICATE KEY UPDATE
                            daily_income = VALUES(daily_income),
                            daily_yield_rate = VALUES(daily_yield_rate),
                            annual_apy = VALUES(annual_apy)
                        `;
                        await connection.query(insertQuery, [walletAddress, previousDate, dailyIncome, dailyYieldRate, annualApy]);
                        logInfo(`Inserted/Updated personal yield rate for ${walletAddress} on ${previousDate}`);
                    }
                }

                logInfo(`Processing event: ${event.event} - ${event.transactionHash}`);

                if (event.event === 'Deposited') {
                    // Проверяем наличие записи в availableToWithdraw для более точной суммы USD
                    const [withdrawRecords] = await connection.query(
                        `SELECT availableToWithdraw FROM availableToWithdraw 
                         WHERE transactionHash = ? AND event = 'Deposited'`,
                        [event.transactionHash]
                    );

                    if (withdrawRecords.length > 0) {
                        totalDepositedUSD += parseFloat(withdrawRecords[0].availableToWithdraw);
                        logInfo(`Deposited - Using availableToWithdraw value: ${withdrawRecords[0].availableToWithdraw}`);
                    } else {
                        const depositedUSD = parseFloat(returnValues.amounts.DAI) + parseFloat(returnValues.amounts.USDC) + parseFloat(returnValues.amounts.USDT);
                        totalDepositedUSD += depositedUSD;
                        logInfo(`Deposited - Calculated value: ${depositedUSD}`);
                    }
                    totalLpShares += parseFloat(returnValues.lpShares);
                    logInfo(`Updated totalDepositedUSD: ${totalDepositedUSD}, totalLpShares: ${totalLpShares}`);
                } else if (event.event === 'Transfer') {
                    // Проверяем наличие записи в availableToWithdraw для более точной суммы USD
                    const [withdrawRecords] = await connection.query(
                        `SELECT availableToWithdraw FROM availableToWithdraw 
                         WHERE transactionHash = ? AND event = 'Transfer'`,
                        [event.transactionHash]
                    );

                    const usdValue = withdrawRecords.length > 0 
                        ? parseFloat(withdrawRecords[0].availableToWithdraw) 
                        : parseFloat(returnValues.usdValue);
                    const lpValue = parseFloat(returnValues.value);

                    if (returnValues.from === walletAddress) {
                        totalDepositedUSD -= usdValue;
                        totalLpShares -= lpValue;
                        logInfo(`Transfer - Sent: ${usdValue} USD, ${lpValue} LP`);
                    } else if (returnValues.to === walletAddress) {
                        totalDepositedUSD += usdValue;
                        totalLpShares += lpValue;
                        logInfo(`Transfer - Received: ${usdValue} USD, ${lpValue} LP`);
                    }
                    logInfo(`Updated totalDepositedUSD: ${totalDepositedUSD}, totalLpShares: ${totalLpShares}`);
                } else if (event.event === 'Withdrawn') {
                    const [withdrawRecords] = await connection.query(
                        `SELECT availableToWithdraw FROM availableToWithdraw 
                         WHERE transactionHash = ? AND event = 'Withdrawn'`,
                        [event.transactionHash]
                    );

                    const withdrawnLpShares = parseFloat(returnValues.lpShares);
                    const sharePercentage = withdrawnLpShares / totalLpShares;

                    if (withdrawRecords.length > 0) {
                        const withdrawnUSD = parseFloat(withdrawRecords[0].availableToWithdraw);
                        totalDepositedUSD -= withdrawnUSD;
                        logInfo(`Withdrawn - Using availableToWithdraw value: ${withdrawnUSD}`);
                    } else {
                        const withdrawnUSD = totalDepositedUSD * sharePercentage;
                        totalDepositedUSD -= withdrawnUSD;
                        logInfo(`Withdrawn - Calculated value: ${withdrawnUSD}`);
                    }
                    totalLpShares -= withdrawnLpShares;
                    logInfo(`Updated totalDepositedUSD: ${totalDepositedUSD}, totalLpShares: ${totalLpShares}`);
                }

                previousDate = eventDateOnly;
            }

            // Обработка последнего дня
            if (previousDate) {
                const [apyRecords] = await connection.query(
                    `SELECT apy FROM apy_info 
                     WHERE DATE(timestamp) = DATE(?)`,
                    [previousDate]
                );

                if (apyRecords.length > 0) {
                    const dailyYieldRate = parseFloat(apyRecords[0].apy) / 365 / 100;
                    const dailyIncome = totalDepositedUSD * dailyYieldRate;
                    totalIncome += dailyIncome;

                    const annualApy = (Math.pow((1 + dailyYieldRate), 365) - 1) * 100;

                    const insertQuery = `
                        INSERT INTO personal_yield_rate (depositor_address, date, daily_income, daily_yield_rate, annual_apy)
                        VALUES (?, ?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE
                        daily_income = VALUES(daily_income),
                        daily_yield_rate = VALUES(daily_yield_rate),
                        annual_apy = VALUES(annual_apy)
                    `;
                    await connection.query(insertQuery, [walletAddress, previousDate, dailyIncome, dailyYieldRate, annualApy]);
                    logInfo(`Inserted/Updated personal yield rate for ${walletAddress} on ${previousDate}`);
                }
            }
        }

        logSuccess("Personal yield rates calculated and saved successfully.");
    } catch (error) {
        logError("Failed to calculate personal yield rates:", error);
    } finally {
        if (connection) connection.release();
    }
}


// Создаем cron-задачу для ежедневного обновления персональной ставки доходности
cron.schedule('0 0 * * *', async () => {
    logInfo('Calculating personal yield rates...');
    await calculatePersonalYieldRate();
});

// Endpoint для получения данных по всем кошелькам
app.get('/wallets-apy', async (req, res) => {
    let connection;

    try {
        connection = await pool.getConnection();

        const [rows] = await connection.query(
            `SELECT depositor_address, date, daily_income, daily_yield_rate, annual_apy FROM personal_yield_rate ORDER BY date DESC`
        );

        res.json(rows);
    } catch (error) {
        logError(`Failed to fetch personal yield rates: ${error}`);
        res.status(500).send('Internal Server Error');
    } finally {
        if (connection) {
            connection.release();
        }
    }
});


// Endpoint для получения данных по конкретному кошельку
app.get('/wallets-apy/:walletAddress', async (req, res) => {
    let connection;

    try {
        const walletAddress = req.params.walletAddress;
        connection = await pool.getConnection();

        const [rows] = await connection.query(
            `SELECT depositor_address, date, daily_income, daily_yield_rate, annual_apy 
             FROM personal_yield_rate 
             WHERE depositor_address = ? 
             ORDER BY date DESC`,
            [walletAddress]
        );

        res.json(rows);
    } catch (error) {
        logError(`Failed to fetch personal yield rates for ${req.params.walletAddress}: ${error}`);
        res.status(500).send('Internal Server Error');
    } finally {
        if (connection) {
            connection.release();
        }
    }
});


// Расчет дохода DSF через получение USDT на адрес стратегии, а также реальныхсумм при выводе пользователей

// Функция для получения всех транзакций DAI по указанному адресу
async function getDAITransactions(address) {
        
    const response = await retryEtherscan(async () => {
        const apiKey = getNextEtherscanApiKey();
        const url = `https://api.etherscan.io/api?module=account&action=tokentx&address=${address}&contractaddress=0x6B175474E89094C44Da98b954EedeAC495271d0F&apikey=${apiKey}`;
        return await axios.get(url);
    });
    
    if (response.data.status === '1' && response.data.message === 'OK') {
        //console.log(`DAI transactions for address ${address}`, response.data.result);
        return response.data.result;
    } else {
        console.warn(`No DAI transactions found for address ${address}`);
        return [];
    }
}

// Функция для получения всех транзакций USDC по указанному адресу
async function getUSDCTransactions(address) {
        
    const response = await retryEtherscan(async () => {
        const apiKey = getNextEtherscanApiKey();
        const url = `https://api.etherscan.io/api?module=account&action=tokentx&address=${address}&contractaddress=0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48&apikey=${apiKey}`;
        return await axios.get(url);
    });
    
    if (response.data.status === '1' && response.data.message === 'OK') {
        //console.log(`USDC transactions for address ${address}`, response.data.result);
        return response.data.result;
    } else {
        console.warn(`No USDC transactions found for address ${address}`);
        return [];
    }
}

// Функция для получения всех транзакций USDT по указанному адресу
async function getUSDTTransactions(address) {
        
    const response = await retryEtherscan(async () => {
        const apiKey = getNextEtherscanApiKey();
        const url = `https://api.etherscan.io/api?module=account&action=tokentx&address=${address}&contractaddress=0xdac17f958d2ee523a2206206994597c13d831ec7&apikey=${apiKey}`;
        return await axios.get(url);
    });
    
    if (response.data.status === '1' && response.data.message === 'OK') {
        //console.log(`USDT transactions for address ${address}`, response.data.result);
        return response.data.result;
    } else {
        console.warn(`No USDT transactions found for address ${address}`);
        return [];
    }
}

// Функция для получения пополнений и выводов DAI по указанному блоку для указанного адреса
async function getDAITransactionsByBlock(address, blockNumber) {

    // console.log('Тип данных address:', typeof address);
    // console.log('Тип данных blockNumber:', typeof blockNumber);
    // console.log('Значение address:', address);
    // console.log('Значение blockNumber:', blockNumber);

    try {
        const transactions = await getDAITransactions(address);
        const filteredTransactions = transactions.filter(tx => tx.blockNumber === blockNumber);

        const deposits = filteredTransactions.filter(tx => tx.to.toLowerCase() === address.toLowerCase());
        const withdrawals = filteredTransactions.filter(tx => tx.from.toLowerCase() === address.toLowerCase());

        const totalDeposits = deposits.reduce((sum, tx) => sum + Number(formatBigInt(tx.value, 18)), 0);
        const totalWithdrawals = withdrawals.reduce((sum, tx) => sum + Number(formatBigInt(tx.value, 18)), 0);

        const balanceDifference = totalDeposits - totalWithdrawals;

        // console.log(` - Deposits for address ${address} in block ${blockNumber}:`, deposits);
        // console.log(` - Withdrawals for address ${address} in block ${blockNumber}:`, withdrawals);
        // console.log(` - Total Deposits: ${totalDeposits}, Total Withdrawals: ${totalWithdrawals}`);
        // console.log(` - Balance Difference: ${balanceDifference}`);

        return balanceDifference;
    } catch (error) {
        logError(`Failed to get DAI transactions for address ${address} in block ${blockNumber}: ${error}`);
        throw new Error('Failed to get DAI transactions');
    }
}

// Функция для получения пополнений и выводов USDC по указанному блоку для указанного адреса
async function getUSDCTransactionsByBlock(address, blockNumber) {

    // console.log('Тип данных address:', typeof address);
    // console.log('Тип данных blockNumber:', typeof blockNumber);
    // console.log('Значение address:', address);
    // console.log('Значение blockNumber:', blockNumber);

    try {
        const transactions = await getUSDCTransactions(address);
        const filteredTransactions = transactions.filter(tx => tx.blockNumber === blockNumber);

        const deposits = filteredTransactions.filter(tx => tx.to.toLowerCase() === address.toLowerCase());
        const withdrawals = filteredTransactions.filter(tx => tx.from.toLowerCase() === address.toLowerCase());

        const totalDeposits = deposits.reduce((sum, tx) => sum + Number(formatBigInt(tx.value, 6)), 0);
        const totalWithdrawals = withdrawals.reduce((sum, tx) => sum + Number(formatBigInt(tx.value, 6)), 0);

        const balanceDifference = totalDeposits - totalWithdrawals;

        // console.log(` - Deposits for address ${address} in block ${blockNumber}:`, deposits);
        // console.log(` - Withdrawals for address ${address} in block ${blockNumber}:`, withdrawals);
        // console.log(` - Total Deposits: ${totalDeposits}, Total Withdrawals: ${totalWithdrawals}`);
        // console.log(` - Balance Difference: ${balanceDifference}`);

        return balanceDifference;
    } catch (error) {
        logError(`Failed to get USDT transactions for address ${address} in block ${blockNumber}: ${error}`);
        throw new Error('Failed to get USDT transactions');
    }
}

// Функция для получения пополнений и выводов USDT по указанному блоку для указанного адреса
async function getUSDTTransactionsByBlock(address, blockNumber) {

    // console.log('Тип данных address:', typeof address);
    // console.log('Тип данных blockNumber:', typeof blockNumber);
    // console.log('Значение address:', address);
    // console.log('Значение blockNumber:', blockNumber);

    try {
        const transactions = await getUSDTTransactions(address);
        const filteredTransactions = transactions.filter(tx => tx.blockNumber === blockNumber);

        const deposits = filteredTransactions.filter(tx => tx.to.toLowerCase() === address.toLowerCase());
        const withdrawals = filteredTransactions.filter(tx => tx.from.toLowerCase() === address.toLowerCase());

        const totalDeposits = deposits.reduce((sum, tx) => sum + Number(formatBigInt(tx.value, 6)), 0);
        const totalWithdrawals = withdrawals.reduce((sum, tx) => sum + Number(formatBigInt(tx.value, 6)), 0);

        const balanceDifference = totalDeposits - totalWithdrawals;

        // console.log(` - Deposits for address ${address} in block ${blockNumber}:`, deposits);
        // console.log(` - Withdrawals for address ${address} in block ${blockNumber}:`, withdrawals);
        // console.log(` - Total Deposits: ${totalDeposits}, Total Withdrawals: ${totalWithdrawals}`);
        // console.log(` - Balance Difference: ${balanceDifference}`);

        return balanceDifference;
    } catch (error) {
        logError(`Failed to get USDT transactions for address ${address} in block ${blockNumber}: ${error}`);
        throw new Error('Failed to get USDT transactions');
    }
}

// Функция для перебора нескольких адресов Стратегий и вызова getUSDTTransactionsByBlock для каждого адреса
async function getBalanceDifferencesForAddresses(addresses, blockNumber) {

    if (!addresses || !Array.isArray(addresses)) {
        logError(`addresses должен быть массивом.`);
        throw new Error('addresses должен быть массивом.');
    }
    if (!blockNumber || typeof blockNumber !== 'string') {
        logError(`blockNumber должен быть строкой.`);
        throw new Error('blockNumber должен быть строкой.');
    }

    let totalBalanceDifference = 0;

    for (const address of addresses) {
        if (!address || typeof address !== 'string') {
            logError(`address должен быть строкой.`);
            throw new Error('address должен быть строкой.');
        }
        try {
            const balanceDifference = await getUSDTTransactionsByBlock(address, blockNumber);
            //console.log(` -  - Balance Difference: ${balanceDifference}`);
            totalBalanceDifference += Number(balanceDifference);
        } catch (error) {
            logError(`Failed to get balance difference for address ${address} in block ${blockNumber}: ${error}`);
        }
    }

    return totalBalanceDifference;
}

// Новый маршрут для получения пополнений и выводов USDT по указанному блоку
app.get('/usdt/:address/:blockNumber', async (req, res) => {
    const { address, blockNumber } = req.params;

    try {
        const result = await getUSDTTransactionsByBlock(address, blockNumber);
        res.json(result);
    } catch (error) {
        console.error(error);
        res.status(500).send('Failed to get USDT transactions');
    }
});

//
//
// Определение для Депозитов и Выводов ( статусы Optimized и Standard )
//
//

// Функция для получения всех транзакций по указанному адресу и блоку
async function getTransactionsByAddressAndBlock(address, blockNumber) {

    const response = await retryEtherscan(async () => {
        const apiKey = getNextEtherscanApiKey();
        const url = `https://api.etherscan.io/api?module=account&action=txlist&address=${address}&startblock=${blockNumber}&endblock=${blockNumber}&sort=asc&apikey=${apiKey}`;
        return await axios.get(url);
    });

    if (response.data.status === '1' && response.data.message === 'OK') {
        return response.data.result;
    } else {
        console.warn(`No transactions found for address ${address} in block ${blockNumber}`);
        return [];
    }
}

// Функция для получения всех транзакций с участием одного адреса в указанном блоке
async function showBlockTransactions(address, blockNumber) {
    try {
        // Получаем все транзакции для данного адреса и блока
        const transactions = await getTransactionsByAddressAndBlock(address, blockNumber);

        // Возвращаем 'Optimized', если есть транзакции, иначе 'Standard'
        return transactions.length > 0 ? 'Optimized' : 'Standard';
    } catch (error) {
        logError(`Failed to get transactions for address ${address} in block ${blockNumber}: ${error}`);
        throw new Error('Failed to get transactions');
    }
}

// Обновленный маршрут для отображения всех транзакций в указанном блоке
app.get('/ckeck_optimized/:blockNumber', async (req, res) => {
    const address = '0xa68be02a2B7EbeCac84a6F33F2DDa63B05911b95';
    const { blockNumber } = req.params;
    logWarning(`blockNumber : ${blockNumber}`)
    try {
        const transactions = await showBlockTransactions(address, blockNumber);
        console.log(`Transactions in block ${blockNumber}:`, transactions);
        res.json(transactions);
    } catch (error) {
        console.error(error);
        res.status(500).send('Failed to get transactions for block');
    }
});

//
//
// Чистый доход DSF
//
//

// Новый маршрут для получения данных о доходах и стоимости транзакций
app.get('/event-summary', async (req, res) => {
    try {
        // Получение всех событий "AutoCompoundAll" и вычисление TotalIncomeDSF и AutoCompoundAllCostUsd
        const [autoCompoundAllEvents] = await pool.query(`
            SELECT SUM(CAST(returnValues->'$.incomeDSF' AS DECIMAL(18, 8))) AS TotalIncomeDSF, SUM(transactionCostUsd) AS AutoCompoundAllCostUsd
            FROM contract_events
            WHERE event = 'AutoCompoundAll'
        `);

        // Получение всех событий "ClaimedAllManagementFee" и вычисление ClaimedAllManagementFeeCostUsd
        const [claimedAllManagementFeeEvents] = await pool.query(`
            SELECT SUM(transactionCostUsd) AS ClaimedAllManagementFeeCostUsd
            FROM contract_events
            WHERE event = 'ClaimedAllManagementFee'
        `);

        // Получение всех уникальных событий "Deposited" с "transaction_status": "Optimized" и вычисление DepositedCostUsd
        const [depositedEvents] = await pool.query(`
            SELECT SUM(transactionCostUsd) AS DepositedCostUsd
            FROM (
                SELECT DISTINCT blockNumber, transactionCostUsd
                FROM contract_events
                WHERE event = 'Deposited' AND JSON_UNQUOTE(returnValues->'$.transaction_status') = 'Optimized'
            ) AS uniqueDepositedEvents
        `);

        // Получение всех уникальных событий "Withdrawn" с "transaction_status": "Optimized" и вычисление WithdrawnCostUsd
        const [withdrawnEvents] = await pool.query(`
            SELECT SUM(transactionCostUsd) AS WithdrawnCostUsd
            FROM (
                SELECT DISTINCT blockNumber, transactionCostUsd
                FROM contract_events
                WHERE event = 'Withdrawn' AND JSON_UNQUOTE(returnValues->'$.transaction_status') = 'Optimized'
            ) AS uniqueWithdrawnEvents
        `);

        const totalIncomeDSF = autoCompoundAllEvents[0].TotalIncomeDSF || 0;
        const autoCompoundAllCostUsd = autoCompoundAllEvents[0].AutoCompoundAllCostUsd || 0;
        const claimedAllManagementFeeCostUsd = claimedAllManagementFeeEvents[0].ClaimedAllManagementFeeCostUsd || 0;
        const depositedCostUsd = depositedEvents[0].DepositedCostUsd || 0;
        const withdrawnCostUsd = withdrawnEvents[0].WithdrawnCostUsd || 0;

        const netIncome = totalIncomeDSF - autoCompoundAllCostUsd - claimedAllManagementFeeCostUsd - depositedCostUsd - withdrawnCostUsd;

        const summary = {
            TotalIncomeDSF: totalIncomeDSF,
            AutoCompoundAllCostUsd: autoCompoundAllCostUsd,
            ClaimedAllManagementFeeCostUsd: claimedAllManagementFeeCostUsd,
            DepositedCostUsd: depositedCostUsd,
            WithdrawnCostUsd: withdrawnCostUsd,
            NetIncome: netIncome
        };

        res.json(summary);
    } catch (error) {
        console.error('Failed to fetch event summary:', error);
        res.status(500).send('Failed to fetch event summary');
    }
});

// Новый маршрут для получения сводной информации о событиях по каждому месяцу
app.get('/monthly-event-summary', async (req, res) => {
    try {
        // Получение сводной информации по каждому месяцу
        const [monthlySummary] = await pool.query(`
            SELECT 
                DATE_FORMAT(eventDate, '%Y-%m') AS month,
                SUM(CAST(returnValues->'$.incomeDSF' AS DECIMAL(18, 8))) AS TotalIncomeDSF,
                SUM(CASE WHEN event = 'AutoCompoundAll' THEN transactionCostUsd ELSE 0 END) AS AutoCompoundAllCostUsd,
                SUM(CASE WHEN event = 'ClaimedAllManagementFee' THEN transactionCostUsd ELSE 0 END) AS ClaimedAllManagementFeeCostUsd,
                SUM(CASE 
                    WHEN event = 'Deposited' AND JSON_UNQUOTE(returnValues->'$.transaction_status') = 'Optimized' 
                    THEN transactionCostUsd 
                    ELSE 0 
                END) AS DepositedCostUsd,
                SUM(CASE 
                    WHEN event = 'Withdrawn' AND JSON_UNQUOTE(returnValues->'$.transaction_status') = 'Optimized' 
                    THEN transactionCostUsd 
                    ELSE 0 
                END) AS WithdrawnCostUsd
            FROM contract_events
            GROUP BY month
            ORDER BY month ASC
        `);

        // Вычисление чистого дохода для каждого месяца
        const summaryWithNetIncome = monthlySummary.map(monthly => {
            const netIncome = (monthly.TotalIncomeDSF || 0) - (monthly.AutoCompoundAllCostUsd || 0) - (monthly.ClaimedAllManagementFeeCostUsd || 0) - (monthly.DepositedCostUsd || 0) - (monthly.WithdrawnCostUsd || 0);
            return {
                ...monthly,
                NetIncome: netIncome
            };
        });

        res.json(summaryWithNetIncome);
    } catch (error) {
        console.error('Failed to fetch monthly event summary:', error);
        res.status(500).send('Failed to fetch monthly event summary');
    }
});

//
//
// DSF доходы с каждого кошелька 
//
//

// Сбор данных из unique_depositors и таблицы событий
async function fetchUniqueDepositors() {
    const query = `SELECT depositor_address FROM unique_depositors`;
    const [rows] = await pool.query(query);
    console.log(`Fetched ${rows.length} unique depositors`);
    return rows.map(row => row.depositor_address);
}

// Сбор данных из contract_events и таблицы событий
async function fetchAllEvents() {
    const query = `SELECT * FROM contract_events WHERE event IN ('Deposited', 'Withdrawn', 'Transfer', 'AutoCompoundAll') ORDER BY blockNumber ASC`;
    const [rows] = await pool.query(query);
    console.log(`Fetched ${rows.length} events`);
    return rows;
}

// Расчет доли дохода проекта DSF от каждого кошелька
async function calculateIncomeDSF() {
    const uniqueDepositors = await fetchUniqueDepositors();
    const events = await fetchAllEvents();

    let totalLpShares = 0.0; // Общее количество lpShares
    const lpShares = new Map(); // Хранение lpShares для каждого кошелька
    const incomeDSFMap = new Map(); // Хранение дохода для каждого кошелька

    // Инициализация доходов всех кошельков нулями
    for (const wallet of uniqueDepositors) {
        incomeDSFMap.set(wallet, 0.0);
    }

    for (const event of events) {
        try {
            const returnValues = event.returnValues;

            if (event.event === 'Deposited') {
                const shares = parseFloat(returnValues.lpShares || 0);

                totalLpShares += shares;
                lpShares.set(returnValues.depositor, (lpShares.get(returnValues.depositor) || 0.0) + shares);
                console.log(`Deposited: ${shares} shares to ${returnValues.depositor}, totalLpShares: ${totalLpShares}`);
            } else if (event.event === 'Withdrawn') {
                const shares = parseFloat(returnValues.lpShares || 0);

                totalLpShares -= shares;
                lpShares.set(returnValues.withdrawer, (lpShares.get(returnValues.withdrawer) || 0.0) - shares);
                console.log(`Withdrawn: ${shares} shares from ${returnValues.withdrawer}, totalLpShares: ${totalLpShares}`);
            } else if (event.event === 'Transfer') {
                const shares = parseFloat(returnValues.value || 0);

                lpShares.set(returnValues.from, (lpShares.get(returnValues.from) || 0.0) - shares);
                lpShares.set(returnValues.to, (lpShares.get(returnValues.to) || 0.0) + shares);
                console.log(`Transfer: ${shares} shares from ${returnValues.from} to ${returnValues.to}`);
            } else if (event.event === 'AutoCompoundAll') {
                const incomeDSF = parseFloat(returnValues.incomeDSF || 0);
                logWarning(`AutoCompoundAll: incomeDSF ${incomeDSF} at block ${event.blockNumber}`);

                for (const wallet of uniqueDepositors) {
                    const walletLpShares = lpShares.get(wallet) || 0.0;
                    const walletIncomeBefore = incomeDSFMap.get(wallet) || 0.0;
                    
                    if (walletLpShares > 0.0) {
                        const walletShare = walletLpShares / totalLpShares;
                        const walletIncome = (incomeDSF * walletShare);

                        // Суммируем текущий walletIncome с предыдущими
                        incomeDSFMap.set(wallet, walletIncomeBefore + walletIncome);

                        //incomeDSFMap.set(wallet, (incomeDSFMap.get(wallet) || 0.0) + walletIncome);
                        console.log(`AutoCompoundAll: wallet ${wallet}, walletIncome ${walletIncome}, totalWalletIncome ${incomeDSFMap.get(wallet)}`);
                    } else {
                        console.log(`AutoCompoundAll: wallet ${wallet}, walletIncome 0.0, totalWalletIncome ${incomeDSFMap.get(wallet)}`);
                    }
                }
            }
        } catch (error) {
            console.error(`Failed to process event ${event.id}: ${error.message}`);
        }
    }

    logSuccess(`Complete calculate Income DSF`)
    await storeAllIncomeDSF(incomeDSFMap);
}

// Обновление данных в таблице incomDSFfromEveryOne
async function storeAllIncomeDSF(incomeDSFMap) {
    const query = `
        INSERT INTO incomDSFfromEveryOne (wallet_address, incomeDSF)
        VALUES ?
        ON DUPLICATE KEY UPDATE incomeDSF = VALUES(incomeDSF)
    `;
    const values = Array.from(incomeDSFMap.entries()).map(([wallet, income]) => [wallet, income.toString()]);
    
    try {
        const result = await pool.query(query, [values]);
        console.log(`Stored all incomeDSF with result: ${JSON.stringify(result)}`);
    } catch (error) {
        console.error(`Failed to store all incomeDSF: ${error.message}`);
    }
}

// Эндпоинт для получения всех данных доходов DSF с каждого кошелька 
app.get('/api/incomDSFfromEveryOne', async (req, res) => {
    try {
        const query = `SELECT * FROM incomDSFfromEveryOne`;
        const [rows] = await pool.query(query);
        console.log(`Fetched incomDSFfromEveryOne data: ${JSON.stringify(rows)}`);
        res.json(rows);
    } catch (error) {
        console.error(`Failed to fetch incomDSFfromEveryOne data: ${error.message}`);
        res.status(500).send('Failed to fetch data');
    }
});

// Эндпоинт для получения данных доходов DSF с конкретного кошелька 
app.get('/api/incomDSFfromEveryOne/:wallet', async (req, res) => {
    const { wallet } = req.params;
    try {
        const query = `SELECT * FROM incomDSFfromEveryOne WHERE wallet_address = ?`;
        const [rows] = await pool.query(query, [wallet]);
        console.log(`Fetched incomDSFfromEveryOne data for wallet ${wallet}: ${JSON.stringify(rows)}`);
        res.json(rows[0] || {});
    } catch (error) {
        console.error(`Failed to fetch incomDSFfromEveryOne data for wallet ${wallet}: ${error.message}`);
        res.status(500).send('Failed to fetch data');
    }
});


// Эндпоинт для получения суммы incomeDSF
app.get('/api/incomDSFfromEveryOne/total', async (req, res) => {
    try {
        const query = `SELECT SUM(incomeDSF) as totalIncomeDSF FROM incomDSFfromEveryOne`;
        const [rows] = await pool.query(query);
        const totalIncomeDSF = rows[0].totalIncomeDSF || 0;
        console.log(`Fetched total incomeDSF: ${totalIncomeDSF}`);
        res.json({ totalIncomeDSF });
    } catch (error) {
        console.error(`Failed to fetch total incomeDSF: ${error.message}`);
        res.status(500).send('Failed to fetch data');
    }
});


//
//
//
//
//


// Последовательный запуск всех функций при запуске сервера
// NEW Apy + Wallets
const updateAllData = async () => {
    try {

        initializationCompleted = false;

        await checkAllApiKeys();

        // Проверка на упущенные Events
        ///await initializeMissingEvents().catch(console.error);

        // Инициализация таблицы и заполнение уникальными депозиторами
        ///await populateUniqueDepositors();

        //await updateApyData();
        //logSuccess("APY data updated successfully.");

        // Расчет персональных APY
        //await calculatePersonalYieldRate();

        // Обновление данныхкошельков
        ///await updateAllWallets();
        //logSuccess("Wallets updated successfully.");

        // Расчет доходов DSF с каждого кошелька
        ///await calculateIncomeDSF();

        initializationTelegramBotEvents = true;
        initializationCompleted = true;
        console.log(`checkForNewEvents : activated (${initializationCompleted})`);

    } catch (error) {
        logError(`Failed to update all data: ${error}`);
    }
};

const port = process.env.PORT || 3000;
const server = app.listen(port, () => {

    console.log(`\n${colors.blue}${`    ▄▄▄▄▄▄▄▄▄      ▄▄▄▄▄▄▄    ▄▄▄▄▄▄▄▄▄▄▄ `}${colors.reset}`);
    console.log(`${colors.blue}${`    ███▀▀▀▀███▄  ▄██▀▀▀▀███▄  ███▀▀▀▀▀▀▀▀ `}${colors.reset}`);
    console.log(`${colors.blue}${`    ███     ███  ██▄     ▀▀▀  ███         `}${colors.reset}`);
    console.log(`${colors.blue}${`    ███     ███   ▀███████▄   █████████   `}${colors.reset}`);
    console.log(`${colors.blue}${`    ███     ███  ▄▄▄     ▀██  ███         `}${colors.reset}`);
    console.log(`${colors.blue}${`    ███▄▄▄▄███▀  ▀██▄▄▄▄▄██▀  ███         `}${colors.reset}`);
    console.log(`${colors.blue}${`    ▀▀▀▀▀▀▀▀▀      ▀▀▀▀▀▀▀    ▀▀▀         `}${colors.reset}`);
    console.log(`\n${colors.blue}${`    --- Defining  Successful  Future --- `}${colors.reset}\n`);
   
    logWarning(`\nServer is listening on port ${port}`);
    updateAllData(); // Запуск последовательного обновления данных

    ///setInterval(checkForNewEvents, 30000);  // Проверка каждые 30 секунд
});

// Увеличение таймаута соединения
server.keepAliveTimeout = 120000; // 120 секунд
server.headersTimeout = 121000; // 121 секунда
