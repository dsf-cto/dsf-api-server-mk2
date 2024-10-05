// DSF.Finance API Server Mk6.6.6.5.2
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
import NodeCache from 'node-cache';

import pLimit from 'p-limit'; // Библиотека для ограничения параллельных запросов
import { performance } from 'perf_hooks'; // Для измерения времени выполнения

const ethPriceCache = new NodeCache({ stdTTL: 3600 }); // Кеш с временем жизни 1 час (3600 секунд)
const userDepositsCache = new NodeCache({ stdTTL: 86400 }); // Кэш с TTL 86400 секунд (24 часа)
const apyCache = new NodeCache({ stdTTL: 10800 }); // 10800  секунд = 3 часа

const llama = new NodeCache({ stdTTL: 10800 }); // АПИ из дефиламы без изменений


// Параметры конфигурации для pLimit
const MAX_CONCURRENT_REQUESTS = 5; // Максимальное количество одновременных запросов
const REQUEST_DELAY_MS = 100; // Задержка между запросами в миллисекундах
const RETRY_DELAY_MS = 3000; // Задержка перед повторной попыткой в миллисекундах

// Флаги 
let initializationCompleted = false; // Изначально инициализация не завершена
let initializationTelegramBotEvents = false; // Изначально Телеграм бот Уведомляющий об эвентах не запущен

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

// Параметр `trust proxy` в true
//app.set('trust proxy', true);
// app.set('trust proxy', false); // для локального сервера
app.set('trust proxy', 1); // для продакшн 1 означает доверять первому proxy, если за ним стоит несколько proxy, можно использовать 'trust proxy', 'loopback' или 'loopback, linklocal, uniquelocal'


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

// Проверка доступности сервера базы данных
async function testConnection() {
    let connection;
    try {
        connection = await pool.getConnection();
        logSuccess(`\nУспешное подключение к базе данных!\n`);
    } catch (error) {
        logError(`\nНе удалось подключиться к базе данных : ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

testConnection();

// For RESTART DataBase apy_info:  
// const dropTableQuery = `DROP TABLE IF EXISTS apy_info;`;
//         await pool.query(dropTableQuery);
//         console.log(`Таблица успешно удалена`);

// For RESTART DataBase incomDSFfromEveryOne:  
// const dropTableQuery = `DROP TABLE IF EXISTS incomDSFfromEveryOne;`;
//         await pool.query(dropTableQuery);
//         console.log(`Таблица успешно удалена`);

// For RESTART DataBase settings:  
// const dropTableQuery = `DROP TABLE IF EXISTS settings;`;
//         await pool.query(dropTableQuery);
//         console.log(`Таблица успешно удалена`);

// For RESTART DataBase wallet_info: 
// const dropTableQuery = `DROP TABLE IF EXISTS wallet_info;`;
//         await pool.query(dropTableQuery);
//         console.log(`Таблица успешно удалена`);

// For RESTART DataBase contract_events: 
const dropTableQuery = `DROP TABLE IF EXISTS contract_events;`;
        await pool.query(dropTableQuery);
        console.log(`Таблица успешно удалена`);

// For RESTART DataBase personal_yield_rate: 
// const dropTableQuery = `DROP TABLE IF EXISTS personal_yield_rate;`;
//         await pool.query(dropTableQuery);
//         console.log(`Таблица успешно удалена`);

// For RESTART DataBase transactionsWOCoin: 
// const dropTableQuery2 = `DROP TABLE IF EXISTS transactionsWOCoin;`;
//         await pool.query(dropTableQuery2);
//         console.log(`Таблица успешно удалена`);

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
        apy_today DECIMAL(36, 6) NOT NULL,
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
        logError(`\nFailed to create 'wallet_info' table : ${error}\n`);
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
        logSuccess(`\nTable 'settings' checked/created successfully.\n`);
    } catch (error) {
        logError(`\nFailed to create 'settings' table : ${error}\n`);
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
        logError(`\nFailed to create 'apy_info' table : ${error}\n`);
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
        logError(`\nFailed to create 'wallet_events' table : ${error}\n`);
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
        logError(`\nFailed to create 'unique_depositors' table : ${error}\n`);
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
        logError(`\nFailed to create 'contract_events' table : ${error}\n`);
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
        logError(`\nFailed to create 'availableToWithdraw' table : ${error}\n`);
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
        logSuccess(`Table 'incomDSFfromEveryOne' checked/created successfully.`);
    } catch (error) {
        logError(`Failed to create 'incomDSFfromEveryOne' table : ${error}`);
    } finally {
        if (connection) connection.release();
    }
}

initializeDatabaseIncomDSF();

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
        logError(`\nFailed to create 'personal_yield_rate' table : ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

initializePersonalYieldRateTable();

// Таблица eth_price_history для кеширования данных о цене ETH в USD
const createEthPriceHistoryTableQuery = `
    CREATE TABLE IF NOT EXISTS eth_price_history (
        id INT AUTO_INCREMENT PRIMARY KEY,
        date DATE NOT NULL,
        price DECIMAL(18, 8) NOT NULL,
        source VARCHAR(50) NOT NULL,
        UNIQUE(date, source)
    );
`;

async function initializeEthPriceHistoryTable() {
    let connection;
    try {
        connection = await pool.getConnection();
        await connection.query(createEthPriceHistoryTableQuery);
        console.log(`\nTable 'eth_price_history' created successfully.\n`);
    } catch (error) {
        console.error(`\nFailed to create 'eth_price_history' table: ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

initializeEthPriceHistoryTable();

// Таблица transactionsWOCoin для кеширования данных о цене ETH в USD
const createTransactionsWOCoinTableQuery = `
    CREATE TABLE IF NOT EXISTS transactionsWOCoin (
    id INT AUTO_INCREMENT PRIMARY KEY,
    txHash VARCHAR(66) NOT NULL,
    depositor VARCHAR(42) NOT NULL,
    lpShares DECIMAL(38, 18) NOT NULL,
    AvailableToWithdraw DECIMAL(38, 18),
    lpPrice DECIMAL(38, 18),
    UNIQUE(txHash, depositor)
);
`;

async function initializeTransactionsWOCoinTable() {
    let connection;
    try {
        connection = await pool.getConnection();
        await connection.query(createTransactionsWOCoinTableQuery);
        console.log(`\nTable 'transactionsWOCoin' created successfully.\n`);
    } catch (error) {
        console.error(`\nFailed to create 'transactionsWOCoin' table: ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

initializeTransactionsWOCoinTable();

const createUserDepositsTableQuery = `
    CREATE TABLE IF NOT EXISTS user_deposits (
        id INT AUTO_INCREMENT PRIMARY KEY,
        wallet_address VARCHAR(255) UNIQUE,
        totalDepositedUSD DECIMAL(36, 18),
        totalLpShares DECIMAL(36, 18),
        lastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX(wallet_address)
    );
`;

//NEW
async function initializeUserDepositsTable() {
    let connection;
    try {
        connection = await pool.getConnection();
        await connection.query(createUserDepositsTableQuery);
        logSuccess(`\nTable 'user_deposits' checked/created successfully.\n`);
    } catch (error) {
        logError(`\nFailed to create 'user_deposits' table : ${error}\n`);
    } finally {
        if (connection) connection.release();
    }
}

//NEW
initializeUserDepositsTable();

// Connect To Web3 Provider

const providers = process.env.PROVIDERS.split(',');

let providerIndex = 0;
let web3;

async function connectToWeb3Provider() {
    try {
        const selectedProvider = providers[providerIndex];
        web3 = new Web3(selectedProvider);
        await web3.eth.net.getId();
        console.log(`Connected to Ethereum network using provider : ${selectedProvider}`);
        
        // Move to the next provider
        providerIndex = (providerIndex + 1) % providers.length;
    } catch (error) {
        logError(`Failed to connect to provider ${providers[providerIndex]} : ${error}`);
        
        providerIndex = (providerIndex + 1) % providers.length; // Go to the next provider
        await connectToWeb3Provider(); // Recursively try to connect to the next ISP
    }
}

// Функция для переключения на следующий провайдер
const switchProvider = async () => {
    providerIndex = (providerIndex + 1) % providers.length;
    await connectToWeb3Provider();
};

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

// Проверка на валидность всех АПИ ключей
async function checkAllApiKeys() {
    const startTime = performance.now();
    logWarning(`\nCheck Etherscan Api Keys\n`);
    
    // Параллельная проверка всех Etherscan API ключей
    const etherscanApiKeyPromises = etherscanApiKeys.map(apiKey => checkEtherscanApiKey(apiKey));
    const etherscanApiKeyResults = await Promise.all(etherscanApiKeyPromises);

    logWarning(`\nCheck Web3 Providers Api Keys\n`);
    
    // Параллельная проверка всех Web3 провайдеров
    const providerPromises = providers.map(provider => checkWeb3Provider(provider));
    const providerResults = await Promise.all(providerPromises);

    const endTime = performance.now();
    console.log(`\nCompleted in ${(endTime - startTime) / 1000} seconds.\n`);
}

// Функция для повторного выполнения запроса с экспоненциальной задержкой
const retry = async (fn, retries = 3, delay = 200) => {
    try {
        await connectToWeb3Provider(); // Обновление провайдера перед следующим запросом
        return await fn();
    } catch (error) {
        if (retries > 0) {
            logWarning(`Retrying... attempts left : ${retries}`);
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
            logWarning(`Retrying... attempts left : ${retries}`);
            etherscanApiKeyIndex = (etherscanApiKeyIndex + 1) % etherscanApiKeys.length;
            //await getNextEtherscanApiKey(); // Обновление провайдера перед повторной попыткой
            await new Promise(res => setTimeout(res, delay));
            return retryEtherscan(fn, retries - 1, delay * 2);
        } else {
            throw error;
        }
    }
};

// Функция для повторного выполнения запроса не Web3 с экспоненциальной задержкой
const retryNotWeb3 = async (fn, retries = 3, delay = 200) => {
    try {
        return await fn();
    } catch (error) {
        if (retries > 0) {
            //console.warn(`Повторная попытка... осталось попыток: ${retries}`);
            console.log(`Retrying Not Web3... attempts left: ${retries}`);
            await new Promise(res => setTimeout(res, delay));
            return retry(fn, retries - 1, delay * 2);
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

    const startTime = performance.now();

    if (!walletAddress_) {
        throw new Error(`\nWalletAddress is not defined`);
    }
    const walletAddress = normalizeAddress(walletAddress_);
    console.log(`\nWallet Address          : ${walletAddress}`);

    // Считаем сэкономленные и потраченные средства кошелька на транзакциях
    const {
        totalEthSpent: ethSpent,
        totalUsdSpent: usdSpent,
        totalEthSaved: ethSaved,
        totalUsdSaved: usdSaved
    } = await calculateSavingsAndSpending(walletAddress_);
    
    const apyToday = await getApyToday().catch(() => 0);

    let ratioUser;
    try {
        // Получаем коэффициент LP пользователя
        ratioUser = await retry(() => ratioContract.methods.calculateLpRatio(walletAddress).call());
    } catch (error) {
        logError(`Error fetching ratio: ${error}`);
        ratioUser = 0;
    }

    // Если ratioUser равно 0, возвращаем значения по умолчанию и логируем предупреждения
    if (ratioUser === 0) {
        logWarningsForDefaultValues(ethSpent, usdSpent, ethSaved, usdSaved, apyToday);
        return getDefaultWalletData(ethSpent, usdSpent, ethSaved, usdSaved, apyToday);
    }

    
    try {
        // Запрашиваем необходимые данные параллельно
        const [availableToWithdraw, dsfLpBalance, userDeposits, crvEarned, cvxTotalCliffs, cvx_totalSupply, cvx_reductionPerCliff, cvx_balanceOf, crv_balanceOf] = await Promise.all([
            retry(() => contractDSFStrategy.methods.calcWithdrawOneCoin(ratioUser, 2).call()).catch(() => 0), // Если все попытки завершатся неудачно, значение по умолчанию 0 будет использовано
            retry(() => contractDSF.methods.balanceOf(walletAddress).call()).catch(() => 0),
            getCurrentDeposit(walletAddress).catch(() => 0),
            retry(() => cvxRewardsContract.methods.earned(contractsLib.DSFStrategy).call()).catch(() => 0),
            retry(() => config_cvxContract.methods.totalCliffs().call()).catch(() => 0),
            retry(() => config_cvxContract.methods.totalSupply().call()).catch(() => 0),
            retry(() => config_cvxContract.methods.reductionPerCliff().call()).catch(() => 0),
            retry(() => config_cvxContract.methods.balanceOf(contractsLib.DSFStrategy).call()).catch(() => 0),
            retry(() => config_crvContract.methods.balanceOf(contractsLib.DSFStrategy).call()).catch(() => 0)
        ]);
    
        const cvxRemainCliffs = cvxTotalCliffs - cvx_totalSupply / cvx_reductionPerCliff;
        const amountInCVX = (crvEarned * cvxRemainCliffs) / cvxTotalCliffs + cvx_balanceOf;
        const amountInCRV = crvEarned + crv_balanceOf;

        const { crvShare, cvxShare } = calculateShares(amountInCRV, amountInCVX, ratioUser);
        const { crvCost, cvxCost } = await getShareCosts(crvShare, cvxShare);

        const availableToWithdraw_ = Number(availableToWithdraw) / 1e6;

        const annualYieldRate = await calculateWeightedYieldRate(walletAddress, availableToWithdraw_, cvxCost, crvCost, userDeposits);

        const response = {
            userDeposits,
            dsfLpBalance: (Number(dsfLpBalance) / 1e18).toPrecision(18),
            safeRatioUser: (parseFloat(ratioUser) / 1e16).toPrecision(16),
            availableToWithdraw: availableToWithdraw_,
            cvxShare: Number(cvxShare) / 1e18,
            cvxCost,
            crvShare: Number(crvShare) / 1e18,
            crvCost,
            annualYieldRate,
            apyToday,
            ethSpent,
            usdSpent,
            ethSaved,
            usdSaved
        };

        logWalletData(response);

        return response;
    } catch (error) {
        logError(`Error retrieving data for wallet: ${walletAddress}, ${error}`);
        logWarningsForDefaultValues(ethSpent, usdSpent, ethSaved, usdSaved, apyToday);
        return getDefaultWalletData(ethSpent, usdSpent, ethSaved, usdSaved, apyToday);
    }
}

// Выводит информацию если баланс 0 или если произошла ошибка
function logWarningsForDefaultValues(ethSpent, usdSpent, ethSaved, usdSaved, apyToday) {
    logWarning("userDeposits       USDT : 0");
    logWarning("dsfLpBalance     DSF LP : 0");
    logWarning("ratioUser             % : 0");
    logWarning("availableWithdraw  USDT : 0");
    logWarning("cvxShare            CVX : 0");
    logWarning("cvxCost            USDT : 0");
    logWarning("crvShare            CRV : 0");
    logWarning("crvCost            USDT : 0");
    logWarning("annualYieldRate       % : 0");
    logWarning(`apyToday              % : ${apyToday}`);
    logWarning(`ethSpent            ETH : ${ethSpent}`);
    logWarning(`usdSpent              $ : ${usdSpent}`);
    logWarning(`ethSaved            ETH : ${ethSaved}`);
    logWarning(`usdSaved              $ : ${usdSaved}`);
}

// Возвращает значения по умолчанию, если произошла ошибка
function getDefaultWalletData(ethSpent, usdSpent, ethSaved, usdSaved, apyToday) {
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
        apyToday,
        ethSpent,
        usdSpent,
        ethSaved,
        usdSaved
    };
}

// Рассчитывает доли CRV и CVX на основе данных и коэффициента пользователя
function calculateShares(amountInCRV, amountInCVX, ratioUser) {
    const crvShare = Math.trunc(Number(amountInCRV) * Number(ratioUser) / 1e18 * 0.85);
    const cvxShare = Math.trunc(Number(amountInCVX) * Number(ratioUser) / 1e18 * 0.85);
    return { crvShare, cvxShare };
}

// Получает стоимость долей CRV и CVX
async function getShareCosts(crvShare, cvxShare) {
    let crvCost = 0;
    let cvxCost = 0;

    if (crvShare > 20000 && cvxShare > 20000) {
        const [crvCostArray, cvxCostArray] = await Promise.all([
            retry(() => routerContract.methods.getAmountsOut(Math.trunc(crvShare), crvToUsdtPath).call()),
            retry(() => routerContract.methods.getAmountsOut(Math.trunc(cvxShare), cvxToUsdtPath).call())
        ]);

        crvCost = Number(crvCostArray[crvCostArray.length - 1]) / 1e6;
        cvxCost = Number(cvxCostArray[cvxCostArray.length - 1]) / 1e6;
    }

    return { crvCost, cvxCost };
}

// Возвращает значения кошелька если все хорошо
function logWalletData(data) {
    console.log(`userDeposits       USDT : ${data.userDeposits}`);
    console.log(`dsfLpBalance     DSF LP : ${data.dsfLpBalance}`);
    console.log(`safeRatioUser         % : ${data.safeRatioUser}`);
    console.log(`availableWithdraw  USDT : ${data.availableToWithdraw}`);
    console.log(`cvxShare            CVX : ${data.cvxShare}`);
    console.log(`cvxCost            USDT : ${data.cvxCost}`);
    console.log(`crvShare            CRV : ${data.crvShare}`);
    console.log(`crvCost            USDT : ${data.crvCost}`);
    console.log(`annualYieldRate       % : ${data.annualYieldRate}`);
    console.log(`apyToday              % : ${data.apyToday}`);
    console.log(`ethSpent            ETH : ${data.ethSpent}`);
    console.log(`usdSpent              $ : ${data.usdSpent}`);
    console.log(`ethSaved            ETH : ${data.ethSaved}`);
    console.log(`usdSaved              $ : ${data.usdSaved}`);
}

// Функция для получения последнего значения APY и умножения его на 0.85
async function getApyToday() {
    try {
        // Проверка кэша перед запросом
        let cachedApy = apyCache.get('apyToday');
        if (cachedApy) {
            console.log('APY retrieved from cache');
            return cachedApy;
        }

        const response = await axios.get('https://yields.llama.fi/chart/8a20c472-142c-4442-b724-40f2183c073e');
        const data = response.data.data;

        if (data.length === 0) {
            throw new Error('No APY data available');
        }

        const latestApy = data[data.length - 1].apy;
        const apyToday = latestApy * 0.85;

        // Кэширование результата
        apyCache.set('apyToday', apyToday);

        return apyToday;
    } catch (error) {
        logError(`Error fetching APY data: ${error}`);
        return 0; // Значение по умолчанию
    }
}

async function getWalletDataOptim(walletAddress_, cachedData) {

    const startTime = performance.now();

    if (!walletAddress_) {
        throw new Error(`\nWalletAddress is not defined`);
    }
    const walletAddress = normalizeAddress(walletAddress_);
    console.log(`\nWallet Address          : ${walletAddress}`);

    // Считаем сэкономленные и потраченные средства кошелька на транзакциях
    const { 
        totalEthSpent: ethSpent, 
        totalUsdSpent: usdSpent, 
        totalEthSaved: ethSaved, 
        totalUsdSaved: usdSaved 
    } = await calculateSavingsAndSpending(walletAddress_);

    let ratioUser;
    await delay(1000); // Задержка 1 секунда

    try {
        ratioUser = await retry(() => ratioContract.methods.calculateLpRatio(walletAddress).call());
    } catch (error) {
        logError(`Error occurred while fetching ratio: ${error}`);
        ratioUser = 0;
    }

    // Если ratioUser равно 0, возвращаем значения по умолчанию и логируем предупреждения
    if (ratioUser === 0) {
        logWarningsForDefaultValues(ethSpent, usdSpent, ethSaved, usdSaved, cachedData.apyToday);
        return getDefaultWalletData(ethSpent, usdSpent, ethSaved, usdSaved, cachedData.apyToday);
    }

    try {
        const [availableToWithdraw, dsfLpBalance] = await Promise.all([
            retry(() => contractDSFStrategy.methods.calcWithdrawOneCoin(ratioUser, 2).call()).catch(() => 0),
            retry(() => contractDSF.methods.balanceOf(walletAddress).call()).catch(() => 0)
        ]);
        
        const availableToWithdraw_ = Number(availableToWithdraw) / 1e6;

        const userDeposits = await getCurrentDeposit(walletAddress);
        const { crvShare, cvxShare } = calculateShares(cachedData.amountInCRV, cachedData.amountInCVX, ratioUser);
        const { crvCost, cvxCost } = await getShareCosts(crvShare, cvxShare);

        const annualYieldRate = await calculateWeightedYieldRate(walletAddress, availableToWithdraw_, cvxCost, crvCost, userDeposits);
        
        const response = {
            userDeposits,
            dsfLpBalance: (Number(dsfLpBalance) / 1e18).toPrecision(18),
            safeRatioUser: (parseFloat(ratioUser) / 1e16).toPrecision(16),
            availableToWithdraw: availableToWithdraw_,
            cvxShare: Number(cvxShare) / 1e18,
            cvxCost,
            crvShare: Number(crvShare) / 1e18,
            crvCost,
            annualYieldRate,
            apyToday: cachedData.apyToday,
            ethSpent,
            usdSpent,
            ethSaved,
            usdSaved
        };

        logWalletData(response);

        const endTime = performance.now();
        console.log(`\nUpdate Wallet Address completed (${(endTime - startTime) / 1000} seconds)\n`);

        return response;
    } catch (error) {

        const endTime = performance.now();
        console.log(`\nUpdate Wallet Address completed (${(endTime - startTime) / 1000} seconds)\n`);

        logError(`Error retrieving data for wallet: ${walletAddress}, ${error}`);
        logWarningsForDefaultValues(ethSpent, usdSpent, ethSaved, usdSaved, cachedData.apyToday);
        return getDefaultWalletData(ethSpent, usdSpent, ethSaved, usdSaved, cachedData.apyToday);
    }
}
    
// Функция для нормализации адреса Ethereum
function normalizeAddress(address) {
    if (web3.utils.isAddress(address)) {
        return web3.utils.toChecksumAddress(address);
    } else {
        logError(`Invalid Ethereum address : ${address}`);
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
    const startTime = performance.now();

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
            logInfo(`No events found for depositor : ${walletAddress}`);
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
                    logError(`Failed to parse returnValues for event : ${event.transactionHash}, error : ${error}`);
                    continue;
                }
            }

            //logInfo(`Processing event : ${event.event} - ${event.transactionHash}`);

            if (event.event === 'Deposited') {
                if (totalDepositedUSD < 0) totalDepositedUSD = 0; // Защита от отрицательных значений

                if (returnValues.placed !== null && !isNaN(parseFloat(returnValues.placed)) && parseFloat(returnValues.placed) !== 0) {
                    totalDepositedUSD += parseFloat(returnValues.placed);
                } else {
                    const depositedUSD = parseFloat(returnValues.amounts.DAI) + parseFloat(returnValues.amounts.USDC) + parseFloat(returnValues.amounts.USDT);
                    totalDepositedUSD += depositedUSD - (depositedUSD * 0.0016); // Вычитаем 0.16% комиссии
                }
                totalLpShares += parseFloat(returnValues.lpShares);
                logInfo(`Updated totalDepositedUSD : ${totalDepositedUSD}, totalLpShares : ${totalLpShares}`);
            } else if (event.event === 'Transfer') {
                if (totalDepositedUSD < 0) totalDepositedUSD = 0; // Защита от отрицательных значений          

                const usdValue = parseFloat(returnValues.usdValue);
                const lpValue = parseFloat(returnValues.value);

                if (returnValues.from === walletAddress) {
                    totalDepositedUSD -= usdValue;
                    totalLpShares -= lpValue;

                    if (totalDepositedUSD < 0) totalDepositedUSD = 0; // Защита от отрицательных значений
                    if (totalLpShares < 0) totalLpShares = 0; // Защита от отрицательных значений
                } else if (returnValues.to === walletAddress) {
                    if (returnValues.placed !== null && !isNaN(parseFloat(returnValues.placed)) && parseFloat(returnValues.placed) !== 0) {
                        totalDepositedUSD += parseFloat(returnValues.placed);
                    } else {
                        totalDepositedUSD += usdValue;
                    }
                    totalLpShares += lpValue;
                }
                logInfo(`Updated totalDepositedUSD : ${totalDepositedUSD}, totalLpShares : ${totalLpShares}`);
            } else if (event.event === 'Withdrawn') {
                const withdrawnLpShares = parseFloat(returnValues.lpShares);
                const sharePercentage = withdrawnLpShares / totalLpShares;

                const withdrawnUSD = totalDepositedUSD * sharePercentage;
                totalDepositedUSD -= withdrawnUSD;

                if (totalDepositedUSD < 0) totalDepositedUSD = 0; // Защита от отрицательных значений
                if (totalLpShares < 0) totalLpShares = 0; // Защита от отрицательных значений

                totalLpShares -= withdrawnLpShares;
                logInfo(`Updated totalDepositedUSD : ${totalDepositedUSD}, totalLpShares : ${totalLpShares}`);
            }

            // } else if (event.event === 'Withdrawn') {
            //     const withdrawnLpShares = parseFloat(returnValues.lpShares);
            //     const sharePercentage = withdrawnLpShares / totalLpShares;

            //     let withdrawnUSD;

            //     // Проверка наличия "realWithdrawnAmount" в returnValues
            //     if (returnValues.realWithdrawnAmount) {
            //         const realWithdrawnDAI = parseFloat(returnValues.realWithdrawnAmount.DAI || 0);
            //         const realWithdrawnUSDC = parseFloat(returnValues.realWithdrawnAmount.USDC || 0);
            //         const realWithdrawnUSDT = parseFloat(returnValues.realWithdrawnAmount.USDT || 0);
            
            //         // Суммируем реальные суммы вывода в USD
            //         withdrawnUSD = realWithdrawnDAI + realWithdrawnUSDC + realWithdrawnUSDT;
            //     } else {
            //         // Если "realWithdrawnAmount" отсутствует, рассчитываем по проценту доли
            //         withdrawnUSD = totalDepositedUSD * sharePercentage;
            //     }                
                
            //     totalDepositedUSD -= withdrawnUSD;

            //     if (totalDepositedUSD < 0) totalDepositedUSD = 0; // Защита от отрицательных значений
            //     if (totalLpShares < 0) totalLpShares = 0; // Защита от отрицательных значений

            //     totalLpShares -= withdrawnLpShares;
            //     logInfo(`Updated totalDepositedUSD : ${totalDepositedUSD}, totalLpShares : ${totalLpShares}`);
            // }
        }

        // Обновляем информацию в user_deposits
        await connection.query(
            `INSERT INTO user_deposits (wallet_address, totalDepositedUSD, totalLpShares, lastUpdated)
             VALUES (?, ?, ?, NOW())
             ON DUPLICATE KEY UPDATE totalDepositedUSD = VALUES(totalDepositedUSD), totalLpShares = VALUES(totalLpShares), lastUpdated = NOW()`,
            [walletAddress, totalDepositedUSD, totalLpShares]
        );

        logSuccess(`Calculated current deposit for ${walletAddress} is ${totalDepositedUSD}`);
        return totalDepositedUSD;
    } catch (error) {
        logError(`Failed to calculate current deposit : ${error}`);
        return 0;
    } finally {
        if (connection) connection.release();

        const endTime = performance.now();
        logWarning(`calculateCurrentDeposit completed (${(endTime - startTime) / 1000} seconds)`)
    }
}

// Функция для расчета и обновления депозитов для всех кошельков
async function updateAllDeposits() {
    let connection;
    try {
        connection = await pool.getConnection();

        // Получаем все уникальные адреса депозиторов
        const [uniqueDepositors] = await connection.query('SELECT depositor_address FROM unique_depositors');
        
        for (const depositor of uniqueDepositors) {
            await calculateCurrentDeposit(depositor.depositor_address);
        }

        // Обновляем кэш после расчета всех депозитов
        await updateUserDepositsCache();
    } catch (error) {
        logError(`Failed to update all deposits : ${error}`);
    } finally {
        if (connection) connection.release();
    }
}

// Функция для обновления кэша данных депозитов
async function updateUserDepositsCache() {
    try {
        const [rows] = await pool.query('SELECT * FROM user_deposits ORDER BY lastUpdated DESC');
        userDepositsCache.set('userDeposits', rows);
        console.log('User deposits cache updated');
    } catch (error) {
        console.error(`Failed to update user deposits cache: ${error.message}`);
    }
}

// Функция для получения текущего депозита из кэша или пересчета
async function getCurrentDeposit(walletAddress) {
    try {
        let cachedData = userDepositsCache.get('userDeposits');
        if (!cachedData) {
            await updateUserDepositsCache();
            cachedData = userDepositsCache.get('userDeposits');
        }
        
        const userDeposit = cachedData.find(deposit => deposit.wallet_address === walletAddress);
        if (userDeposit) {
            return userDeposit.totalDepositedUSD;
        } else {
            logWarning(`Not cachedData for ${walletAddress}`)
            return await calculateCurrentDeposit(walletAddress);
        }
    } catch (error) {
        console.error(`Failed to get current deposit for wallet ${walletAddress}: ${error.message}`);
        return 0;
    }
}

// Эндпоинт для получения текущего депозита конкретного пользователя
app.get('/user-deposit/:walletAddress', async (req, res) => {
    const walletAddress = req.params.walletAddress;
    try {
        const userDeposits = await getCurrentDeposit(walletAddress);
        res.json({ walletAddress, userDeposits });
    } catch (error) {
        console.error(`Failed to get user deposit: ${error.message}`);
        res.status(500).send('Failed to get user deposit');
    }
});

// Эндпоинт для получения текущих депозитов всех пользователей
app.get('/user-deposits', async (req, res) => {
    try {
        let cachedData = userDepositsCache.get('userDeposits');
        if (!cachedData) {
            await updateUserDepositsCache();
            cachedData = userDepositsCache.get('userDeposits');
        }
        res.json(cachedData);
    } catch (error) {
        console.error(`Failed to fetch user deposits: ${error.message}`);
        res.status(500).send('Failed to fetch user deposits');
    }
});

// Функция для расчета средневзвешенной ставки дохода
async function calculateWeightedYieldRate(walletAddress, availableToWithdraw, cvxCost, crvCost, userDeposits) {
    
    const startTime = performance.now();

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
            logInfo(`События не найдены для депозитора : ${walletAddress}`);
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
                    logError(`Не удалось разобрать returnValues для события : ${event.transactionHash}, error : ${error}`);
                    continue;
                }
            }

            const eventDate = new Date(event.eventDate);
            const eventDateOnly = eventDate.toISOString().split('T')[0];

            logInfo(`Обработка события : ${event.event} - ${event.transactionHash} на ${eventDateOnly}`);

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
                    logInfo(`${event.event} - Использование значения availableToWithdraw : ${withdrawRecords[0].availableToWithdraw}`);
                } else {
                    depositedUSD = parseFloat(returnValues.amounts.DAI) + parseFloat(returnValues.amounts.USDC) + parseFloat(returnValues.amounts.USDT);
                    depositedUSD -= depositedUSD * 0.0016; // Вычитаем 0.16% из суммы депозита
                    logInfo(`${event.event} - Рассчитанная сумма : ${depositedUSD} после вычета 0.16%`);
                }

                if (lastEventDate) {
                    const daysActive = (eventDate - lastEventDate) / (1000 * 60 * 60 * 24);
                    weightedDepositDays += totalDepositedUSD * daysActive;
                    logInfo(`Добавлено ${daysActive} активных дней для totalDepositedUSD : ${totalDepositedUSD}, взвешенные дни депозита теперь : ${weightedDepositDays}`);
                }

                totalDepositedUSD += depositedUSD;
                totalLpShares += parseFloat(returnValues.lpShares);
                lastEventDate = eventDate;
                logInfo(`Обновлено totalDepositedUSD : ${totalDepositedUSD}, totalLpShares : ${totalLpShares}, взвешенные дни депозита : ${weightedDepositDays}`);
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
                        logInfo(`Добавлено ${daysActive} активных дней для totalDepositedUSD : ${totalDepositedUSD}, взвешенные дни депозита теперь : ${weightedDepositDays}`);
                    }

                    totalDepositedUSD -= usdValue;
                    totalLpShares -= lpValue;

                    if (totalDepositedUSD < 0) totalDepositedUSD = 0;
                    if (totalLpShares < 0) totalLpShares = 0;

                    if (lastEventDate) {
                        const daysActive = (eventDate - lastEventDate) / (1000 * 60 * 60 * 24);
                        weightedDepositDays -= usdValue * daysActive;
                        logInfo(`Вычтено ${daysActive} активных дней для withdrawnUSD : ${usdValue}, взвешенные дни депозита теперь : ${weightedDepositDays}`);
                    }
                    logInfo(`Transfer - Sent : ${usdValue} USD, ${lpValue} LP`);
                } else if (returnValues.to === walletAddress) {
                    if (lastEventDate) {
                        const daysActive = (eventDate - lastEventDate) / (1000 * 60 * 60 * 24);
                        weightedDepositDays += totalDepositedUSD * daysActive;
                        logInfo(`Добавлено ${daysActive} активных дней для totalDepositedUSD : ${totalDepositedUSD}, взвешенные дни депозита теперь : ${weightedDepositDays}`);
                    }

                    totalDepositedUSD += usdValue;
                    totalLpShares += lpValue;
                    logInfo(`Transfer - Received : ${usdValue} USD, ${lpValue} LP`);
                }
                lastEventDate = eventDate;
                logInfo(`Обновлено totalDepositedUSD : ${totalDepositedUSD}, totalLpShares : ${totalLpShares}, взвешенные дни депозита : ${weightedDepositDays}`);
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
                    logInfo(`${event.event} - Использование значения availableToWithdraw : ${withdrawRecords[0].availableToWithdraw}`);
                } else {
                    withdrawnUSD = totalDepositedUSD * sharePercentage;
                    logInfo(`${event.event} - Рассчитанная сумма : ${withdrawnUSD}`);
                }

                if (lastEventDate) {
                    const daysActive = (eventDate - lastEventDate) / (1000 * 60 * 60 * 24);
                    weightedDepositDays += totalDepositedUSD * daysActive;
                    logInfo(`Добавлено ${daysActive} активных дней для totalDepositedUSD : ${totalDepositedUSD}, взвешенные дни депозита теперь : ${weightedDepositDays}`);
                }

                totalDepositedUSD -= withdrawnUSD;
                totalLpShares -= withdrawnLpShares;

                if (totalDepositedUSD < 0) totalDepositedUSD = 0;
                if (totalLpShares < 0) totalLpShares = 0;

                if (lastEventDate) {
                    const daysActive = (eventDate - lastEventDate) / (1000 * 60 * 60 * 24);
                    weightedDepositDays -= withdrawnUSD * daysActive;
                    logInfo(`Вычтено ${daysActive} активных дней для withdrawnUSD : ${withdrawnUSD}, взвешенные дни депозита теперь : ${weightedDepositDays}`);
                }

                if (totalDepositedUSD === 0) {
                    weightedDepositDays = 0;
                    logInfo(`Депозит стал равен нулю, обнуляем взвешенные дни депозита.`);
                }

                lastEventDate = eventDate;
                logInfo(`Обновлено totalDepositedUSD : ${totalDepositedUSD}, totalLpShares : ${totalLpShares}, взвешенные дни депозита : ${weightedDepositDays}`);
            }
        }

        // Добавляем текущие взвешенные дни депозита до текущей даты
        if (lastEventDate && totalDepositedUSD > 0) {
            const daysActive = (new Date() - lastEventDate) / (1000 * 60 * 60 * 24);
            weightedDepositDays += totalDepositedUSD * daysActive;
            logInfo(`Добавлено ${daysActive} активных дней для totalDepositedUSD : ${totalDepositedUSD} до сегодняшнего дня, взвешенные дни депозита теперь : ${weightedDepositDays}`);
        }

        const totalValue = availableToWithdraw + cvxCost + crvCost - userDeposits;
        logInfo(`Общая сумма из availableToWithdraw, cvxCost и crvCost - userDeposits : ${totalValue}`);

        if (weightedDepositDays === 0) {
            logWarning(`Взвешенные дни депозита равны нулю, возвращаем 0 для ${walletAddress}`);
            return 0;
        }

        const averageDailyRate = totalValue / weightedDepositDays;
        const annualYieldRate = averageDailyRate * 365 * 100;

        logInfo(`Средняя дневная ставка          : ${averageDailyRate}`);
        logInfo(`Рассчитанная годовая доходность : ${annualYieldRate}`);

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
        logError(`Не удалось рассчитать взвешенную ставку доходности : ${error}`);
        return 0;
    } finally {
        if (connection) connection.release();

        const endTime = performance.now();
        logWarning(`calculateSavingsAndSpending completed (${(endTime - startTime) / 1000} seconds)`)
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
        //console.log(`Executing spending query for wallet : ${wallet}`);
        const [spendingRows] = await pool.query(spendingQuery, [wallet, wallet]);
        //console.log(`Spending query result : ${JSON.stringify(spendingRows)}`);
        const totalEthSpent = spendingRows[0].totalEthSpent || 0.0;
        const totalUsdSpent = spendingRows[0].totalUsdSpent || 0.0;
        //console.log(`Total ETH spent : ${totalEthSpent}, Total USD spent : ${totalUsdSpent}`);

        // Расчет сэкономленных средств
        //console.log(`Executing savings query for wallet : ${wallet}`);
        const [savingsRows] = await pool.query(savingsQuery, [wallet, wallet]);
        //console.log(`Savings query result : ${JSON.stringify(savingsRows)}`);
        const totalEthOptimized = savingsRows[0].totalEthOptimized || 0.0;
        const totalUsdOptimized = savingsRows[0].totalUsdOptimized || 0.0;
        //console.log(`Total ETH optimized : ${totalEthOptimized}, Total USD optimized : ${totalUsdOptimized}`);

        // Расчет потраченных средств
        //console.log(`Executing pending costs query for wallet : ${wallet}`);
        const [pendingCostsRows] = await pool.query(pendingCostsQuery, [wallet, wallet]);
        //console.log(`Pending costs query result : ${JSON.stringify(pendingCostsRows)}`);
        const totalEthPending = pendingCostsRows[0].totalEthPending || 0.0;
        const totalUsdPending = pendingCostsRows[0].totalUsdPending || 0.0;
        //console.log(`Total ETH pending : ${totalEthPending}, Total USD pending : ${totalUsdPending}`);

        // Вычисление сэкономленных средств
        const totalEthSaved = Math.max(totalEthOptimized - totalEthPending, 0.0);
        const totalUsdSaved = Math.max(totalUsdOptimized - totalUsdPending, 0.0);

        //console.log(`Total ETH saved : ${totalEthSaved}, Total USD saved : ${totalUsdSaved}`);

        const resultSavingsAndSpending = {
            totalEthSpent: totalEthSpent || 0.0,
            totalUsdSpent: totalUsdSpent || 0.0,
            totalEthSaved: totalEthSaved || 0.0,
            totalUsdSaved: totalUsdSaved || 0.0
        };

        //console.log(`Result for wallet ${wallet} : ${resultSavingsAndSpending}`);
        return resultSavingsAndSpending;
    } catch (error) {
        logError(`Failed to calculate savings and spending for wallet ${wallet} : ${error.message}`);
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

        const totalEthSpent = Math.max(rows[0].totalEthSpent || 0.0, 0.0);
        const totalUsdSpent = Math.max(rows[0].totalUsdSpent || 0.0, 0.0);
        const totalEthSaved = Math.max(rows[0].totalEthSaved || 0.0, 0.0);
        const totalUsdSaved = Math.max(rows[0].totalUsdSaved || 0.0, 0.0);

        return {
            totalEthSpent,
            totalUsdSpent,
            totalEthSaved,
            totalUsdSaved
        };
    } catch (error) {
        logError(`Failed to calculate total savings and spending : ${error}`);
        throw new Error(`Failed to calculate total savings and spending`);
    } finally {
        if (connection) connection.release();
    }
}

// Для getWalletDataOptim
async function updateWalletData(walletAddress, cachedData) {
    
    const startTime = performance.now();

    let connection;
    try {
        // Получение данных кошелька
        const walletData = await getWalletDataOptim(walletAddress, cachedData);

        //console.log(`Retrieved walletData : ${walletData}`);

        // Проверяем, что получены корректные данные
        if (!walletData || typeof walletData !== 'object') {
            throw new Error(`Invalid wallet data retrieved`);
        }

        //console.log(`Retrieved walletData : ${JSON.stringify(walletData)}`);

        // Получение соединения с базой данных
        connection = await pool.getConnection();

        // Проверяем наличие кошелька в таблице wallet_info
        const [rows] = await connection.query('SELECT wallet_address FROM wallet_info WHERE wallet_address = ?', [walletAddress]);
        console.log(`Existing wallet data from database : `, rows);

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
                    apy_today,
                    eth_spent,
                    usd_spent,
                    eth_saved,
                    usd_saved,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())

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
                walletData.apyToday,
                walletData.ethSpent,
                walletData.usdSpent,
                walletData.ethSaved,
                walletData.usdSaved
            ];

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
                apy_today = ?,
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
                walletData.apyToday,
                walletData.ethSpent,
                walletData.usdSpent,
                walletData.ethSaved,
                walletData.usdSaved,
                walletAddress
            ];

            // Выполнение запроса обновления
            await connection.query(updateQuery, updateValues);
            logSuccess(`Data updated for wallet : ${walletAddress}`);
        }
    } catch (error) {
        logError(`Error updating wallet data for ${walletAddress} : ${error}`);
        throw error;
    } finally {
        // Освобождение соединения
        if (connection) connection.release();

        const endTime = performance.now();
        logWarning(`updateWalletData completed (${(endTime - startTime) / 1000} seconds)`)
    }
}

// Для getWalletData 
async function updateWalletDataSingl(walletAddress) {
    let connection;
    try {
        // Проверка, был ли передан адрес кошелька
        if (!walletAddress) {
            throw new Error(`Wallet address is required`);
        }
        
        // Получение данных кошелька
        const walletData = await getWalletData(walletAddress);
        
        // Получение соединения с базой данных
        connection = await pool.getConnection();
        
        // Запрос на обновление данных в базе данных
        // const updateQuery = `
        //     UPDATE wallet_info SET
        //     user_deposits = ?,
        //     dsf_lp_balance = ?,
        //     ratio_user = ?,
        //     available_to_withdraw = ?,
        //     cvx_share = ?,
        //     cvx_cost = ?,
        //     crv_share = ?,
        //     crv_cost = ?,
        //     annual_yield_rate = ?,
        //     apy_today = ?,
        //     eth_spent = ?,
        //     usd_spent = ?,
        //     eth_saved = ?,
        //     usd_saved = ?,
        //     updated_at = NOW()
        //     WHERE wallet_address = ?
        // `;

        const upsertQuery = `
            INSERT INTO wallet_info_test (
                wallet_address, user_deposits, dsf_lp_balance, ratio_user, available_to_withdraw,
                cvx_share, cvx_cost, crv_share, crv_cost, annual_yield_rate, apy_today, 
                eth_spent, usd_spent, eth_saved, usd_saved, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
            ON DUPLICATE KEY UPDATE
                user_deposits = VALUES(user_deposits),
                dsf_lp_balance = VALUES(dsf_lp_balance),
                ratio_user = VALUES(ratio_user),
                available_to_withdraw = VALUES(available_to_withdraw),
                cvx_share = VALUES(cvx_share),
                cvx_cost = VALUES(cvx_cost),
                crv_share = VALUES(crv_share),
                crv_cost = VALUES(crv_cost),
                annual_yield_rate = VALUES(annual_yield_rate),
                apy_today = VALUES(apy_today),
                eth_spent = VALUES(eth_spent),
                usd_spent = VALUES(usd_spent),
                eth_saved = VALUES(eth_saved),
                usd_saved = VALUES(usd_saved),
                updated_at = NOW()
        `;

        // Параметры для запроса обновления
        // const values = [
        //     walletData.userDeposits,
        //     walletData.dsfLpBalance,
        //     walletData.safeRatioUser,
        //     walletData.availableToWithdraw,
        //     walletData.cvxShare,
        //     walletData.cvxCost,
        //     walletData.crvShare,
        //     walletData.crvCost,
        //     walletData.annualYieldRate,
        //     walletData.apyToday,
        //     walletData.ethSpent,
        //     walletData.usdSpent,
        //     walletData.ethSaved,
        //     walletData.usdSaved,
        //     walletAddress
        // ];

        const values = [
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
            walletData.apyToday,
            walletData.ethSpent,
            walletData.usdSpent,
            walletData.ethSaved,
            walletData.usdSaved
        ];

        // Выполнение запроса обновления
        await connection.query(upsertQuery, values);
        logSuccess(`Data updated for wallet : ${walletAddress}`);
    } catch (error) {
        logError(`Error updating wallet data for ${walletAddress} : ${error}`);
        throw error;
    } finally {
        // Освобождение соединения
        if (connection) connection.release();
    }
}

// Функция для обновления данных одного кошелька с повторной попыткой в случае ошибки
async function processWalletUpdate(wallet, cachedData) {
    try {
        await updateWalletData(wallet.wallet_address, cachedData);
        await new Promise(resolve => setTimeout(resolve, REQUEST_DELAY_MS)); // Задержка между запросами
    } catch (error) {
        logError(`Error updating wallet ${wallet.wallet_address}: ${error.message}`);
        // Повторная попытка после задержки
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
        try {
            await updateWalletData(wallet.wallet_address, cachedData);
        } catch (retryError) {
            logError(`Retry failed for wallet ${wallet.wallet_address}: ${retryError.message}`);
        }
    }
}

// Функция для обновления данных всех кошельков с использованием кэшированных данных и ограничения параллельных запросов.
async function updateAllWallets() {
    let connection;
    console.log(`\nStarting update of all wallets...\n`);
    try {
        connection = await pool.getConnection();
        
        // Получаем список адресов из таблицы unique_depositors
        const [wallets] = await connection.query('SELECT depositor_address AS wallet_address FROM unique_depositors');
        console.log(`\n`,wallets);

        // Получаем общие данные один раз параллельно
        const [
            crvEarned,
            cvxTotalCliffs,
            cvx_totalSupply,
            cvx_reductionPerCliff,
            cvx_balanceOf,
            crv_balanceOf
        ] = await Promise.all([
            retry(() => cvxRewardsContract.methods.earned(contractsLib.DSFStrategy).call()),
            retry(() => config_cvxContract.methods.totalCliffs().call()),
            retry(() => config_cvxContract.methods.totalSupply().call()),
            retry(() => config_cvxContract.methods.reductionPerCliff().call()),
            retry(() => config_cvxContract.methods.balanceOf(contractsLib.DSFStrategy).call()),
            retry(() => config_crvContract.methods.balanceOf(contractsLib.DSFStrategy).call())
        ]);

        const cvxRemainCliffs = cvxTotalCliffs - cvx_totalSupply / cvx_reductionPerCliff;
        const amountInCVX = (crvEarned * cvxRemainCliffs) / cvxTotalCliffs + cvx_balanceOf;
        const amountInCRV = crvEarned + crv_balanceOf;

        const apyToday = await getApyToday().catch(() => 0);

        const cachedData = {
            crvEarned,
            cvxTotalCliffs,
            cvx_totalSupply,
            cvx_reductionPerCliff,
            cvx_balanceOf,
            crv_balanceOf,
            cvxRemainCliffs,
            amountInCRV,
            amountInCVX,
            apyToday
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
        
        // Ограничение количества одновременных запросов
        const limit = pLimit(MAX_CONCURRENT_REQUESTS);

        for (const wallet of wallets) {
            await delay(1000); // Задержка 1 секунда ТЕСТ
            await limit(() => processWalletUpdate(wallet, cachedData));
        }

        await limit(() => Promise.resolve()); // Ожидание завершения всех запросов

        logSuccess(`\nAll wallet data updated successfully.`);
    } catch (error) {
        logError(`\nError during initial wallet data update : ${error}`);
    } finally {
        if (connection) connection.release();
    }
}

// Проверить ???
// Эндпоинт для обновления данных кошелька
app.post('/update/:walletAddress', async (req, res) => {
    const walletAddress_ = req.params.walletAddress.toLowerCase();
    const walletAddress = normalizeAddress(walletAddress_);

    if (!walletAddress) {
        logError('Invalid wallet address:', walletAddress_);
        return res.status(400).send('Invalid wallet address');
    }

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
        logError(`Database connection or operation failed : ${error}`);
        res.status(500).send('Internal Server Error');
    } finally {
        // Освобождение соединения
        if (connection) connection.release();
    }
});

// Эндпоинт для получения потраченных и сэкономленных средств для конкретного кошелька
app.get('/wallet/savings/:wallet', async (req, res) => {
    const { wallet } = req.params;

    try {
        const resultSavingsAndSpending = await calculateSavingsAndSpending(wallet);
        res.json(resultSavingsAndSpending);
    } catch (error) {
        logError(`Failed to calculate savings and spending for wallet ${wallet} : ${error.message}`);
        res.status(500).send('Failed to calculate savings and spending');
    }
});

// Эндпоинт для получения суммарных данных о потраченных и сэкономленных средствах со всех кошельков
app.get('/wallet/total-savings', async (req, res) => {
    try {
        const resultTotalSavingsAndSpending = await calculateTotalSavingsAndSpending();
        res.json(resultTotalSavingsAndSpending);
    } catch (error) {
        logError(`Failed to calculate total savings and spending : ${error.message}`);
        res.status(500).send('Failed to calculate total savings and spending');
    }
});

// Эндпоинт для получения всех данных для конкретного кошелька
app.get('/wallet/:walletAddress', async (req, res) => {
    connectToWeb3Provider();

    const walletAddress_ = req.params.walletAddress.toLowerCase();
    const walletAddress = normalizeAddress(walletAddress_);

    const apyToday = await getApyToday().catch(() => 0);

    // Если адрес некорректный, возвращаем значения по умолчанию
    if (!walletAddress) {
        logError('Invalid wallet address:', walletAddress_);
        return res.json(getDefaultWalletData(0, 0, 0, 0, apyToday));
    }
    
    console.log(`\nWallet Address          :`, walletAddress);

    if (!/^(0x)?[0-9a-f]{40}$/i.test(walletAddress)) {
        logError(`Адрес не соответствует ожидаемому формату.`);
    } else {
        logSuccess(`Адрес соответствует ожидаемому формату.`);
    }
    
    let connection;
    try {
        connection = await pool.getConnection();

        dsfLpBalance = retry(() => contractDSF.methods.balanceOf(walletAddress).call()).catch(() => 0);

        // Проверяем наличие кошелька в базе данных unique_depositors
        const [rows] = await connection.query('SELECT * FROM unique_depositors WHERE depositor_address = ?', [walletAddress]);
        //console.log(`Rows from database : ${rows}`);
        
        if (rows.length === 0 && dsfLpBalance === 0) {
            // Если кошелек не найден в unique_depositors и dsfLpBalance равно нулю, возвращаем пустые данные
            logWarning('Wallet not found in unique_depositors.');
            return res.json(getDefaultWalletData(0, 0, 0, 0, apyToday));
        }

        // Проверяем наличие кошелька в базе данных wallet_info
        const [walletRows] = await connection.query('SELECT * FROM wallet_info WHERE wallet_address = ?', [walletAddress]);
        console.log(`Rows from database wallet_info : ${walletRows}`);

        if (walletRows.length === 0) {
            // Если кошелек не найден в wallet_info, получаем данные и сохраняем их
            console.log(`Получаем данные кошелька`);
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
                            apy_today,
                            eth_spent,
                            usd_spent,
                            eth_saved,
                            usd_saved,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
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
                        walletData.apyToday,
                        walletData.ethSpent,
                        walletData.usdSpent,
                        walletData.ethSaved,
                        walletData.usdSaved
                    ]);
                    // Отправляем полученные данные клиенту
                    res.json(serializeBigints(walletData)); // Сериализация и Отправка сериализованных данных
                } else {
                    logWarning(`Wallet balance or ratio is zero, not saving to DB.`);
                    res.json(walletData); // Возвращаем данные без сохранения
                }
            } catch (error) {
                // Логируем ошибку и отправляем ответ сервера
                logError(`Failed to retrieve or insert wallet data : ${error}`);
                res.status(500).send('Internal Server Error');
            }
        } else {
            // Если данные уже есть, возвращаем их
            console.log(`Данные кошелька уже есть`);
            res.json(walletRows[0]);

            // решить
            // добавить запрос на обновление кошелька

        }
    } catch (error) {
        // Обработка ошибок при соединении или выполнении SQL-запроса
        logError(`Database connection or operation failed : ${error}`);
        res.status(500).send('Internal Server Error');
    } finally {
        // Освобождение соединения
        if (connection) connection.release();
    }
});

// Вызов функции обновления всех кошельков раз в 3 часа
cron.schedule('0 */3 * * *', async () => {
    logInfo('UpdateAllWallets - Running a task every 3 hours');
    try {
        await populateUniqueDepositors();
        await updateAllWallets(); // Вызов функции обновления всех кошельков
        //logInfo('All wallets updated successfully.');
    } catch (error) {
        logError('Failed to update all wallets:', error);
    }
});

// NEW Разблокировать после теста
// Создаем cron-задачу для периодического обновления данных APY каждый час
cron.schedule('0 */1 * * *', async () => {
    logInfo('Fetching APY data... Every 1 hour');
    try {
        await addNewDayApyData();
        //logInfo('APY data updated successfully.');
    } catch (error) {
        logError('Failed to update APY data:', error);
    }
}); 

// Создаем cron-задачу для периодического обновления данных APY 
// cron.schedule('* * * * *', async () => {
//     logInfo(`Fetching APY data... Every minute`);
//     await addNewDayApyData(); 
// });

// NEW
// Создаем cron-задачу для периодического обновления данных APY каждую неделю в понедельник в 00:00
cron.schedule('0 0 * * 1', async () => {
    logInfo('Fetching APY data... Every 1 week');
    try {
        await updateApyData();
        //logInfo('Weekly APY data updated successfully.');
    } catch (error) {
        logError('Failed to update weekly APY data:', error);
    }
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
    console.log(`\nStarting update of APY data...\n`);
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

        logSuccess(`\nAPY data fetched and saved successfully.`);
    } catch (error) {
        logError(`\nFailed to fetch or save APY data : ${error}`);
    }
}

// Функция для добавления новых данных APY за день
async function addNewDayApyData() {
    try {
        const response = await axios.get('https://yields.llama.fi/chart/8a20c472-142c-4442-b724-40f2183c073e');
        const data = response.data.data;

        if (!data || data.length === 0) {
            logError(`No data received from the API.`);
            return null;
        }

        const latestEntry = data[data.length - 1];
        const latestTimestamp = new Date(latestEntry.timestamp).toISOString();
        const apy = (latestEntry.apy * 0.85).toFixed(4);

        await upsertApyData(latestTimestamp, apy);

        logSuccess(`\nAPY data fetched and saved successfully.`);
    } catch (error) {
        logError(`\nFailed to fetch or save APY data : ${error}`);
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
        logError(`Database connection or operation failed : ${error}`);
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
        logError(`Failed to update APY data : ${error}`);
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
        logError(`Failed to fetch latest APY data : ${error}`);
        res.status(500).send('Failed to fetch latest APY data');
    }
});

//
//
// Все эвенты
//
//

// Функция для получения исторического курса ETH/USD из базы данных или внешних источников
async function getEthUsdPrice(timestamp) {
    // Преобразуем Unix timestamp (в секундах) в объект Date
    const date = new Date(timestamp * 1000);
    
    // Преобразуем дату в строку формата YYYY-MM-DD
    const dateString = date.toISOString().split('T')[0];

    // Проверяем наличие данных в кеше
    const cachedPrice = ethPriceCache.get(dateString);
    if (cachedPrice) {
        console.log(`Using cached ETH price for ${dateString} : ${cachedPrice}`);
        return cachedPrice; // Возвращаем кэшированную цену, если она есть
    }

    // Проверяем наличие данных в базе данных
    const query = 'SELECT price FROM eth_price_history WHERE date = ?';
    const [rows] = await pool.query(query, [dateString]);

    if (rows.length > 0) {
        const price = rows[0].price;
        console.log(`Using cached ETH price from DB for ${dateString}: ${price}`);
        // Сохраняем данные в кеш
        ethPriceCache.set(dateString, price);
        return price;
    }

    // Если данных нет в базе, делаем запрос к CoinGecko
    try {
        const response = await axios.get('https://api.coingecko.com/api/v3/coins/ethereum/history', {
            params: {
                date: dateString.replace(/-/g, '-') // Передаем дату в формате YYYY-MM-DD
            }
        });

        if (response.data && response.data.market_data && response.data.market_data.current_price && response.data.market_data.current_price.usd) {
            const ethUsdPrice = response.data.market_data.current_price.usd;
            
            // Сохраняем данные в базу
            await pool.query('INSERT INTO eth_price_history (date, price, source) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE price = VALUES(price), source = VALUES(source)', [dateString, ethUsdPrice, 'CoinGecko']);
            
            // Сохраняем данные в кеш
            ethPriceCache.set(dateString, ethUsdPrice);
            console.log(`Fetched and saved ETH price from CoinGecko for ${dateString}: ${ethUsdPrice}`);
            return ethUsdPrice;
        }
    } catch (error) {
        console.error(`Failed to fetch ETH price from CoinGecko: ${error.message}`);
    }

    // Если CoinGecko не смог предоставить данные, делаем запрос к CryptoCompare
    try {
        const response = await axios.get('https://min-api.cryptocompare.com/data/v2/histoday', {
            params: {
                fsym: 'ETH',
                tsym: 'USD',
                limit: 1,
                toTs: timestamp
            }
        });

        if (response.data.Response === 'Success' && response.data.Data && response.data.Data.Data.length > 0) {
            const ethUsdPrice = response.data.Data.Data[0].close;

            // Сохраняем данные в базу
            await pool.query('INSERT INTO eth_price_history (date, price, source) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE price = VALUES(price), source = VALUES(source)', [dateString, ethUsdPrice, 'CryptoCompare']);
            
            // Сохраняем данные в кеш
            ethPriceCache.set(dateString, ethUsdPrice);
            console.log(`Fetched and saved ETH price from CryptoCompare for ${dateString}: ${ethUsdPrice}`);
            return ethUsdPrice;
        }
    } catch (error) {
        console.error(`Failed to fetch ETH price from CryptoCompare: ${error.message}`);
    }

    throw new Error('Failed to fetch ETH price from all sources');
}

// Функция для получения данных из CoinGecko за 365 дней
async function fetchEthPriceFromCoinGecko(days) {
    try {
        const response = await axios.get(`https://api.coingecko.com/api/v3/coins/ethereum/market_chart`, {
            params: {
                vs_currency: 'usd',
                days: days
            }
        });
        if (response.data && response.data.prices) {
            return response.data.prices.map(price => ({
                date: new Date(price[0]).toISOString().split('T')[0],
                price: price[1],
                source: 'CoinGecko'
            }));
        }
    } catch (error) {
        console.error(`Failed to fetch ETH price from CoinGecko: ${error.message}`);
        return [];
    }
}

// Функция для получения данных из CryptoCompare за последние 3 года
async function fetchEthPriceFromCryptoCompare(days) {
    try {
        const response = await axios.get(`https://min-api.cryptocompare.com/data/v2/histoday`, {
            params: {
                fsym: 'ETH',
                tsym: 'USD',
                limit: days
            }
        });
        if (response.data.Response === 'Success' && response.data.Data && response.data.Data.Data) {
            return response.data.Data.Data.map(price => ({
                date: new Date(price.time * 1000).toISOString().split('T')[0],
                price: price.close,
                source: 'CryptoCompare'
            }));
        }
    } catch (error) {
        console.error(`Failed to fetch ETH price from CryptoCompare: ${error.message}`);
        return [];
    }
}

// Функция для сохранения данных в таблицу
async function saveEthPriceData(data) {
    const query = `
        INSERT INTO eth_price_history (date, price, source)
        VALUES ?
        ON DUPLICATE KEY UPDATE price = VALUES(price), source = VALUES(source)
    `;
    const values = data.map(item => [item.date, item.price, item.source]);

    try {
        const result = await pool.query(query, [values]);
        console.log(`Saved ETH price data with result: ${JSON.stringify(result)}`);
    } catch (error) {
        console.error(`Failed to save ETH price data: ${error.message}`);
    }
}

// Функция для инициализации данных о цене ETH из CoinGecko и CryptoCompare
async function initializeEthPriceData() {
    try {
        // Запускаем параллельные запросы к CoinGecko и CryptoCompare
        const [coinGeckoData, cryptoCompareData] = await Promise.all([
            fetchEthPriceFromCoinGecko(365),
            fetchEthPriceFromCryptoCompare(1095)
        ]);

        // Объединение данных
        let ethPriceData = [...cryptoCompareData, ...coinGeckoData];

        // Удаление дубликатов (приоритет на данные из CoinGecko)
        const uniqueData = Array.from(new Map(ethPriceData.map(item => [item.date + item.source, item])).values());

        // Сохранение данных в таблицу
        await saveEthPriceData(uniqueData);
        console.log('ETH price data initialization completed.');
    } catch (error) {
        console.error('Failed to initialize ETH price data:', error);
    }
}

// let initializationCompleted = false; // Изначально инициализация не завершена
// let initializationTelegramBotEvents = false; // Изначально Телеграм бот Уведомляющий об эвентах не запущен

// Функция для начальной и периодической инициализации новых событий
async function initializeMissingEvents() {
    try {
        console.log(`Initializing missing events...`);

        const lastEventBlockQuery = `SELECT MAX(blockNumber) as lastBlock FROM contract_events`;
        const [rows] = await pool.query(lastEventBlockQuery);
        const lastEventBlock = rows[0].lastBlock ? BigInt(rows[0].lastBlock) : BigInt(0);

        const latestBlock = await fetchLatestBlockFromEtherscan();

        if (lastEventBlock >= latestBlock) {
            console.log(`No new blocks to process.`);
            if (!initializationTelegramBotEvents) {
                initializationTelegramBotEvents = true; // Запуск Телеграм бота Уведомляющего об эвентах
            console.log(`telegramBotEvets : activated (${initializationTelegramBotEvents})`);
            }
            return;
        }

        const fromBlock = lastEventBlock + BigInt(1);
        const toBlock = latestBlock;

        const events = await fetchEventsUsingWeb3(fromBlock, toBlock);
        await storeEvents(events);

        console.log(`Fetched and stored missing events up to block ${latestBlock}.`);

        if (!initializationTelegramBotEvents) {
            initializationTelegramBotEvents = true; // Запуск Телеграм бота Уведомляющего об эвентах
            console.log(`telegramBotEvets : activated (${initializationTelegramBotEvents})`);
        }
    } catch (error) {
        logError(`Failed to initialize missing events : ${error}`);
    }
}

//New2
// Функция для записи последнего проверенного блока в базу данных
async function updateLastCheckedBlock(blockNumber) {
    try {
        //console.log(`Updated last checked block to ${blockNumber}`);
        const updateQuery = `INSERT INTO settings (setting_key, value) VALUES ('last_checked_block', ?) ON DUPLICATE KEY UPDATE value = VALUES(value)`;
        await pool.query(updateQuery, [blockNumber.toString()]);
    } catch (error) {
        logError(`Failed to update last checked block : ${error.message}`);
    }
}
//New2
// Функция для получения последнего проверенного блока из базы данных
async function getLastCheckedBlock() {
    const selectQuery = `SELECT value FROM settings WHERE setting_key = 'last_checked_block'`;
    const [rows] = await pool.query(selectQuery);
    return rows.length ? BigInt(rows[0].value) : BigInt(0);
}

// v2
async function checkForNewEvents() {
    if (!initializationCompleted) {
        console.log(`Initialization not completed. Skipping check for new events.`);
        return;
    }

    initializationCompleted = false;

    try {
        logWarning(`Checking for new events...`);

        // Получение номера последнего блока с событиями из базы данных
        const lastEventBlockQuery = `SELECT MAX(blockNumber) as lastBlock FROM contract_events`;
        const [rows] = await pool.query(lastEventBlockQuery);
        const lastEventBlock = rows[0].lastBlock ? BigInt(rows[0].lastBlock) : BigInt(0);

        // Получаем номер последнего блока из Etherscan
        const latestBlock = BigInt(await fetchLatestBlockFromEtherscan());
        
        // Получение номера последнего проверенного блока из таблицы settings
        const lastCheckedBlock = BigInt(await getLastCheckedBlock());

        console.log(`Block Info - Last event block from database: ${lastEventBlock}, Latest block from Etherscan: ${latestBlock}, Last checked block from settings: ${lastCheckedBlock}`);

        // Выбор начального блока для проверки
        //let fromBlock = lastCheckedBlock === BigInt(0) ? lastEventBlock + BigInt(1) : Math.max(lastEventBlock + BigInt(1), lastCheckedBlock + BigInt(1));
        let fromBlock;
        if (lastCheckedBlock === BigInt(0)) {
            fromBlock = lastEventBlock + BigInt(1);
        } else if (lastEventBlock >= lastCheckedBlock) {
            fromBlock = lastEventBlock + BigInt(1);
        } else {
            fromBlock = lastCheckedBlock - BigInt(1000);
        }

        if (fromBlock > latestBlock) {
            console.log(`No new blocks to process.`);
            initializationCompleted = true;
            return;
        }

        let newEventsFound = false;
        let autoCompoundAllFound = false;

        // Проверка новых блоков на события
        while (fromBlock <= latestBlock) {
            const toBlock = fromBlock + BigInt(1000) <= latestBlock ? fromBlock + BigInt(9999) : latestBlock;

            process.stdout.write(`Checking blocks from ${fromBlock} to ${toBlock} - `);

            // Получение событий в диапазоне блоков
            const events = await fetchEventsWithRetry(fromBlock, toBlock);

            if (events.length > 0) {
                newEventsFound = true;

                // Проверяем наличие события AutoCompoundAll
                if (events.some(event => event.event === 'AutoCompoundAll')) {
                    autoCompoundAllFound = true;
                }
                break;  // Прерываем цикл, если найдены новые события
            }
            
            fromBlock = toBlock + BigInt(1);
        }

        if (newEventsFound) {
            console.log(`New events found. Initializing missing events...`);
            await initializeMissingEvents().catch(console.error); // Запускаем поиск всех новых событий

            // Удаление дубликатов событий
            await removeDuplicateEvents();

            // Обновление таблицы уникальных депозиторов
            //await populateUniqueDepositors();

            // Если было найдено событие AutoCompoundAll, запускаем calculateIncomeDSF
            if (autoCompoundAllFound) {
                logWarning(`AutoCompoundAll event found, calculating incomeDSF...`);
                await calculateIncomeDSF();
            }

            // Обновление данных всех кошельков
            await updateAllWallets();
        }

        // Обновляем последний проверенный блок на latestBlock
        await updateLastCheckedBlock(latestBlock);
        console.log(`Last checked block updated to ${latestBlock}`);

    } catch (error) {
        if (error.code === 'PROTOCOL_CONNECTION_LOST') {
            logError(`Connection lost. Reconnecting...`);
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
            logError(`Failed to fetch latest block number from Etherscan : ${response.data}`);
            throw new Error('Failed to fetch latest block number from Etherscan');
        }
    } catch (error) {
        logError(`An error occurred while fetching the latest block number : ${error}`);
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
                logError(`Error fetching events from ${fromBlock} to ${toBlock} : ${error.message}`);
                throw error;
            }
        }
    }
    throw new Error(`Failed to fetch events from ${fromBlock} to ${toBlock} after ${retries} attempts`);
}

// 1
// Функция для получения и записи событий через web3
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
        // console.log(`Event names : ${eventNames.join(', ')}`);

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
            console.log(`Rate limit exceeded, waiting before retrying...`);
            await new Promise(res => setTimeout(res, 20000)); // Ждем 20 секунд перед повторной попыткой
            return fetchEventsUsingWeb3(fromBlock, toBlock);
        } else {
            logError(`Error fetching events from block ${fromBlock} to ${toBlock} : ${error.message}`);
            throw error;
        }
    }
}

// Функция для расчета доступного к выводу значения
async function calculateAvailableToWithdraw(valueOrShares) {
    try {
        // Получаем общее предложение токенов
        const totalSupply_ = await retry(() => contractDSF.methods.totalSupply().call()).catch(() => 0);

        if (totalSupply_ === 0) {
            console.error(`Total supply is zero, returning 0 for available to withdraw.`);
            return 0;
        }

        // Рассчитываем долю пользователя
        const ratioUser_ = valueOrShares / totalSupply_;
        console.log(`\nvalueOrShares`, valueOrShares, `\nratioUser_ = `, ratioUser_);

        // Рассчитываем доступное к выводу значение
        const availableToWithdraw_ = await retry(() => contractDSFStrategy.methods.calcWithdrawOneCoin(ratioUser_, 2).call()).catch(() => 0);

        // Преобразуем значение в float и возвращаем
        return parseFloat(availableToWithdraw_.toString());
    } catch (error) {
        console.error(`Error in calculateAvailableToWithdraw: ${error.message}`);
        return 0; // Возвращаем 0 в случае ошибки
    }
}

// Функция для форматирования больших чисел
function formatBigInt(value, decimals) {
    return (Number(BigInt(value)) / Math.pow(10, decimals)).toFixed(decimals);
}

// тест
// Функция для хранения событий
async function storeEvents(events) {
    const MAX_CONCURRENT_REQUESTS = 6; // Определяем максимальное количество одновременных запросов
    const limit = pLimit(MAX_CONCURRENT_REQUESTS);

    const [transferEvents, otherEvents] = events.reduce(
        ([transfers, others], event) => {
            if (event.event === 'Transfer') {
                transfers.push(event);
            } else {
                others.push(event);
            }
            return [transfers, others];
        },
        [[], []]
    );

    await Promise.all(otherEvents.map(event => limit(() => processEvent(event))));
    await Promise.all(transferEvents.map(event => limit(() => processEvent(event))));
}

// async function storeEvents(events) {

//     // Separate Transfer events from other events
//     const transferEvents = [];
//     const otherEvents = [];

//     for (const event of events) {
//         if (event.event === 'Transfer') {
//             transferEvents.push(event);
//         } else {
//             otherEvents.push(event);
//         }
//     }

//     // Process non-Transfer events first
//     // Сначала обрабатываем события, отличные от Transfer
//     for (const event of otherEvents) {
//         await processEvent(event);
//     }

//     // Process Transfer events
//     // Затем обрабатываем события Transfer
//     for (const event of transferEvents) {
//         await processEvent(event);
//     }
// }

// Функция для добавления задержки
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Функция для обработки событий
async function processEvent(event) {

    await delay(900); // Задержка 900 миллисекунд

    const block = await web3.eth.getBlock(event.blockNumber);
    const eventDate = new Date(Number(block.timestamp) * 1000);
    const transaction = await web3.eth.getTransaction(event.transactionHash);
    const receipt = await web3.eth.getTransactionReceipt(event.transactionHash);
    const gasUsed = BigInt(receipt.gasUsed);
    const gasPrice = BigInt(transaction.gasPrice);
    const transactionCostEth = web3.utils.fromWei((gasUsed * gasPrice).toString(), 'ether');
    const ethUsdPrice = await getEthUsdPrice(Number(block.timestamp));
    const transactionCostUsd = (parseFloat(transactionCostEth) * ethUsdPrice).toFixed(2);

    
    // Выполнение всех асинхронных операций параллельно
    // const [block, transaction, receipt, ethUsdPrice] = await Promise.all([
    //     web3.eth.getBlock(event.blockNumber),
    //     web3.eth.getTransaction(event.transactionHash),
    //     web3.eth.getTransactionReceipt(event.transactionHash),
    //     getEthUsdPrice(Number(block.timestamp))
    // ]);

    // const eventDate = new Date(Number(block.timestamp) * 1000);
    // const gasUsed = BigInt(receipt.gasUsed);
    // const gasPrice = BigInt(transaction.gasPrice);
    // const transactionCostEth = web3.utils.fromWei((gasUsed * gasPrice).toString(), 'ether');
    // const transactionCostUsd = (parseFloat(transactionCostEth) * ethUsdPrice).toFixed(2);

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

            addUniqueDepositor(event.returnValues.to);

            if (initializationTelegramBotEvents) {
                const message = `Event 'CreatedPendingDeposit' detected!
                    \nURL    : https://etherscan.io/tx/${event.transactionHash}
                    \nWallet : ${event.returnValues.depositor}
                    \nDAI    : ${formatBigInt(event.returnValues.amounts[0], 18)}
                    \nUSDC   : ${formatBigInt(event.returnValues.amounts[1], 6)}
                    \nUSDT   : ${formatBigInt(event.returnValues.amounts[2], 6)}`;
                //console.log(message);
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
                //console.log(message);
                sendMessageToChat(message);
            };

            break;
        case 'Deposited': 
            const transactionsD_status = await showBlockTransactions(contractsLib.DSFwallet, blockNumberStr);     
            logError(transactionsD_status)

            // Интеграция processTransaction для событий 'Deposited'
            console.log(`Calling processTransaction for Deposited event. txHash: ${event.transactionHash}, depositor: ${event.returnValues.depositor}, lpShares: ${event.returnValues.lpShares}`);
            logWarning(`depositor: ${event.returnValues.depositor}, lpShares: ${event.returnValues.lpShares}`);

            const availableToWithdrawD = await processTransaction(
                event.transactionHash,
                event.returnValues.depositor,
                event.returnValues.lpShares,
                initializationTelegramBotEvents
            );
            
            addUniqueDepositor(event.returnValues.to);

            logWarning(`availableToWithdrawD ${availableToWithdrawD}`);

            formattedEvent.returnValues = {
                depositor: event.returnValues.depositor,
                amounts: {
                    DAI: formatBigInt(event.returnValues.amounts[0], 18),
                    USDC: formatBigInt(event.returnValues.amounts[1], 6),
                    USDT: formatBigInt(event.returnValues.amounts[2], 6)
                },
                lpShares: formatBigInt(event.returnValues.lpShares, 18),
                transaction_status: transactionsD_status,
                placed: availableToWithdrawD
            };

            if (initializationTelegramBotEvents) {
                const message = `Event 'Deposited ${transactionsD_status}' detected!
                    \nURL    : https://etherscan.io/tx/${event.transactionHash}
                    \nWallet : ${event.returnValues.depositor}
                    \nDSF LP : ${formatBigInt(event.returnValues.lpShares, 18)}
                    \nDAI    : ${formatBigInt(event.returnValues.amounts[0], 18)}
                    \nUSDC   : ${formatBigInt(event.returnValues.amounts[1], 6)}
                    \nUSDT   : ${formatBigInt(event.returnValues.amounts[2], 6)}`;
                //console.log(message);
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
                    const message = `Event 'Withdrawn ${transactionsW_status}' detected!
                        \nURL    : https://etherscan.io/tx/${event.transactionHash}
                        \nWallet : ${event.returnValues.withdrawer}
                        \nDSF LP : ${formatBigInt(event.returnValues.lpShares, 18)}
                        \nDAI    : ${realWithdrawnDAI}
                        \nUSDC   : ${realWithdrawnUSDC}
                        \nUSDT   : ${realWithdrawnUSDT}`;
                    //console.log(message);
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
                const message = `Event 'FailedDeposit ${transactionsFD_status}' detected!
                    \nURL    : https://etherscan.io/tx/${event.transactionHash}
                    \nWallet : ${event.returnValues.depositor}
                    \nDSF LP : ${formatBigInt(event.returnValues.lpShares, 18)}
                    \nDAI    : ${formatBigInt(event.returnValues.amounts[0], 18)}
                    \nUSDC   : ${formatBigInt(event.returnValues.amounts[1], 6)}
                    \nUSDT   : ${formatBigInt(event.returnValues.amounts[2], 6)}`;
                //console.log(message);
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
                const message = `Event 'FailedWithdrawal ${transactionsFW_status}' detected!
                    \nURL    : https://etherscan.io/tx/${event.transactionHash}
                    \nWallet : ${event.returnValues.withdrawer}
                    \nDSF LP : ${formatBigInt(event.returnValues.lpShares, 18)}
                    \nDAI    : ${formatBigInt(event.returnValues.amounts[0], 18)}
                    \nUSDC   : ${formatBigInt(event.returnValues.amounts[1], 6)}
                    \nUSDT   : ${formatBigInt(event.returnValues.amounts[2], 6)}`;
                //console.log(message);
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
                await delay(500); // Задержка пол секунды

                if (initializationTelegramBotEvents) {
                    const message = `Event 'AutoCompoundAll' detected!
                        \nURL    : https://etherscan.io/tx/${event.transactionHash}
                        \nIncome : ${Number(balanceDifference)}`;
                    //console.log(message);
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
                    //console.log(message);
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
                let usdValue;
                usdValue = await calculateTransferUSDValue(event);

                addUniqueDepositor(event.returnValues.to);

                // Интеграция processTransaction для событий 'Transfer'
                console.log(`Calling processTransaction for Deposited event. txHash: ${event.transactionHash}, depositor: ${event.returnValues.depositor}, lpShares: ${event.returnValues.value}`);
                const availableToWithdrawT = await processTransaction(
                    event.transactionHash,
                    event.returnValues.from,
                    event.returnValues.value,
                    initializationTelegramBotEvents
                );
                
                formattedEvent.returnValues = {
                    from: event.returnValues.from,
                    to: event.returnValues.to,
                    value: formatBigInt(event.returnValues.value, 18),
                    usdValue: usdValue,
                    placed: availableToWithdrawT
                };

                if (initializationTelegramBotEvents) {
                    const message = `Event 'Transfer' detected!
                    \nURL    : https://etherscan.io/tx/${event.transactionHash}
                    \nFrom   : ${event.returnValues.from}
                    \nTo     : ${event.returnValues.to}
                    \nDSF LP : ${formatBigInt(event.returnValues.value, 18)}`;
                    //console.log(message);
                    sendMessageToChat(message);
                };

            }
            break;
    }
    await delay(200);
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
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
            eventDate = VALUES(eventDate),
            transactionCostEth = VALUES(transactionCostEth),
            transactionCostUsd = VALUES(transactionCostUsd),
            returnValues = VALUES(returnValues)`,
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
        console.log(`Stored event : ${formattedEvent.event} - ${formattedEvent.transactionHash}`);
    } else {
        console.log(`Event already exists : ${formattedEvent.transactionHash}`);
    }

    // Если событие Transfer или Deposited или Withdrawn, и это новое событие, рассчитываем и записываем availableToWithdraw
    if (initializationTelegramBotEvents && (formattedEvent.event === 'Transfer' || formattedEvent.event === 'Deposited' || formattedEvent.event === 'Withdrawn')) {
        
        // Обновление таблицы уникальных депозиторов
            await populateUniqueDepositors();
        // Обновляем депозиты в кеш и базе данных
        if (formattedEvent.event === 'Transfer') {
            await updateWalletDataSingl(event.returnValues.to);
            await updateWalletDataSingl(event.returnValues.from);

            await updateUserDeposit(event.returnValues.to);
            await updateUserDeposit(event.returnValues.from);
            
        } else {
            await updateWalletDataSingl(event.returnValues.depositor || event.returnValues.withdrawer);
            await updateUserDeposit(event.returnValues.depositor || event.returnValues.withdrawer);
        }
    }
}

// Функция для проверки и удаления дублей
async function removeDuplicateEvents() {
    try {
        // Находим все дублирующиеся события, сгруппированные по идентичным полям event, transactionHash, blockNumber и returnValues
        const query = `
            SELECT event, transactionHash, blockNumber, JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$')) as returnValuesString, COUNT(*) as duplicateCount
            FROM contract_events
            GROUP BY event, transactionHash, blockNumber, returnValuesString
            HAVING duplicateCount > 1
        `;
        const [duplicateEvents] = await pool.query(query);

        if (duplicateEvents.length === 0) {
            console.log('No duplicate events found.');
            return;
        }

        console.log(`Found ${duplicateEvents.length} duplicate event(s).`);

        for (const duplicate of duplicateEvents) {
            const { event, transactionHash, blockNumber, returnValuesString } = duplicate;

            // Находим и удаляем все дублирующиеся записи, кроме одной
            const deleteQuery = `
                DELETE FROM contract_events
                WHERE event = ? AND transactionHash = ? AND blockNumber = ? AND JSON_UNQUOTE(JSON_EXTRACT(returnValues, '$')) = ?
                LIMIT ?
            `;
            await pool.query(deleteQuery, [event, transactionHash, blockNumber, returnValuesString, duplicate.duplicateCount - 1]);

            console.log(`Removed ${duplicate.duplicateCount - 1} duplicate(s) for event ${event} in transaction ${transactionHash}.`);
        }
    } catch (error) {
        console.error('Error while removing duplicate events:', error.message);
    }
}

// Функция для пересчета депозита и обновления кеша
async function updateUserDeposit(walletAddress) {
    const totalDepositedUSD = await calculateCurrentDeposit(walletAddress);

    const [result] = await pool.query(
        `INSERT INTO user_deposits (wallet_address, totalDepositedUSD, totalLpShares)
         VALUES (?, ?, ?)
         ON DUPLICATE KEY UPDATE totalDepositedUSD = VALUES(totalDepositedUSD), totalLpShares = VALUES(totalLpShares)`,
        [walletAddress, totalDepositedUSD, 0] // Обновить totalLpShares на реальное значение, если оно требуется
    );

    userDepositsCache.set(walletAddress, totalDepositedUSD);

    console.log(`Updated deposit for ${walletAddress}: ${totalDepositedUSD}`);
}

// Получаем и сохраням информацио WithdrawOneCoin для конкретных транзакций
async function processTransaction(txHash, depositor, lpShares, status) {
    console.log(`Processing transaction: ${txHash}, depositor: ${depositor}, lpShares: ${lpShares}, status: ${status}`);
    
    try {
        const [existingRecord] = await pool.query('SELECT * FROM transactionsWOCoin WHERE txHash = ? AND depositor = ?', [txHash, depositor]);
        logSuccess(`depositor ${depositor}`)
        const depositor_ = normalizeAddress(depositor);

        console.log(`existingRecord.length`, existingRecord.length);
        console.log(`Query result for existing record: ${JSON.stringify(existingRecord)}`);

        if (existingRecord.length > 0) {
            console.log(`Existing record found: ${JSON.stringify(existingRecord[0])}`);
            return existingRecord[0].AvailableToWithdraw;
        } else {
            console.log(`No existing record found for txHash: ${txHash}, depositor: ${depositor}`);
        }

        let AvailableToWithdraw = 0; // Инициализация с нулевым значением
        let lpPrice = 0; // Инициализация с нулевым значением

        if (status) {
            try {
                AvailableToWithdraw = await calculateAvailableToWithdraw(lpShares);
                logSuccess(`Calculated AvailableToWithdraw : ${AvailableToWithdraw}`);
                lpPrice = await retry(() => contractDSF.methods.lpPrice().call()).catch(() => 0);
                logSuccess(`Fetched lpPrice                : ${lpPrice}`);
                
            } catch (error) {
                console.error(`Failed to Calculated AvailableToWithdraw: ${error.message}`);
            }
        } else {
            try {
                const response = await axios.get(`https://api.dsf.finance/deposit/all/${depositor_}`);
                console.log(`https://api.dsf.finance/deposit/all/${depositor_}`)
                const deposits = response.data.deposits;
                const transaction = deposits.find(deposit => deposit.txHash.toLowerCase() === txHash.toLowerCase());

                console.log(`API response for depositor ${depositor} : ${JSON.stringify(response.data)}`);

                if (transaction) {
                    lpPrice = transaction.lpPrice;
                    AvailableToWithdraw = transaction.amount.toFixed(2);
                    console.log(`Transaction found in API. lpPrice: ${lpPrice}, AvailableToWithdraw: ${AvailableToWithdraw}`);
                } else {
                    console.log(`Transaction not found in API.`);
                    AvailableToWithdraw = 0;
                    lpPrice = 0;
                }
            } catch (error) {
                console.error(`Failed to fetch transaction data from API: ${error.message}`);
                AvailableToWithdraw = 0;
                lpPrice = 0;
            }
        }

        // Проверка на случай, если AvailableToWithdraw все еще undefined или NaN
        if (AvailableToWithdraw === 0 || isNaN(AvailableToWithdraw) || AvailableToWithdraw === undefined || AvailableToWithdraw === null) {
            console.error(`AvailableToWithdraw is NaN or undefined for txHash: ${txHash}, depositor: ${depositor}`);
            AvailableToWithdraw = 0; // Устанавливаем значение по умолчанию
            return 0;
        }

        // Сохранение данных в таблицу
        await pool.query(
            'INSERT INTO transactionsWOCoin (txHash, depositor, lpShares, AvailableToWithdraw, lpPrice) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE AvailableToWithdraw = VALUES(AvailableToWithdraw), lpPrice = VALUES(lpPrice)',
            [txHash, depositor, lpShares.toFixed(2), AvailableToWithdraw.toFixed(2), lpPrice]
        );
        console.log(`Stored transaction data: ${txHash}, AvailableToWithdraw: ${AvailableToWithdraw}, lpPrice: ${lpPrice}`);

        return AvailableToWithdraw;
    } catch (error) {
        console.error(`Error processing transaction: ${error.message}`);
        return 0;
    }
}

// Эндпоинт для получения всех записей из таблицы transactions
app.get('/transactionsWOCoin', async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM transactionsWOCoin ORDER BY id DESC');
        res.json(rows);
    } catch (error) {
        console.error(`Failed to fetch transactions: ${error.message}`);
        res.status(500).send('Failed to fetch transactions');
    }
});


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

    console.log(`Stored availableToWithdraw for event : ${event.event} - ${event.transactionHash}`);
}

// Функция для расчета эквивалента value в USD для события Transfer
async function calculateTransferUSDValue(event) {
    console.log(`Calculating USD value for Transfer event : ${event.transactionHash}`);
    const address = event.returnValues.from;
    let balanceUSD = 0;
    let lpShares = 0;

    const [events] = await pool.query(
        `SELECT * FROM contract_events 
         WHERE JSON_EXTRACT(returnValues, '$.depositor') = ? OR JSON_EXTRACT(returnValues, '$.withdrawer') = ? 
         OR JSON_EXTRACT(returnValues, '$.from') = ? OR JSON_EXTRACT(returnValues, '$.to') = ? 
         ORDER BY eventDate ASC`,
        [address, address, address, address]
    );

    for (const event of events) {
        console.log(`Processing event : ${event.event} - ${event.transactionHash}`);
        let returnValues = event.returnValues;
        if (typeof returnValues === 'string') {
            try {
                returnValues = JSON.parse(returnValues);
            } catch (error) {
                logError(`Failed to parse returnValues for event : ${event.transactionHash}, error : ${error}`);
                continue;
            }
        }

        if (event.event === 'Deposited' && returnValues.depositor === address) {
            if (balanceUSD < 0) balanceUSD = 0; // Защита от отрицательных значений

            if (returnValues.placed !== null && !isNaN(parseFloat(returnValues.placed)) && parseFloat(returnValues.placed) !== 0) {
                balanceUSD += parseFloat(returnValues.placed);
            } else {
                const depositedUSD = parseFloat(returnValues.amounts.DAI) + parseFloat(returnValues.amounts.USDC) + parseFloat(returnValues.amounts.USDT);
                balanceUSD += depositedUSD - (depositedUSD * 0.0016); // Вычитаем 0.16% комиссии
            }
            lpShares += parseFloat(returnValues.lpShares);
            //console.log(`Updated balanceUSD : ${balanceUSD}, lpShares : ${lpShares}`);
        } else if (event.event === 'Withdrawn' && returnValues.withdrawer === address) {
            const withdrawnLpShares = parseFloat(returnValues.lpShares);
            const sharePercentage = withdrawnLpShares / lpShares;
            const withdrawnUSD = balanceUSD * sharePercentage;
            balanceUSD -= withdrawnUSD;
            if (balanceUSD < 0) balanceUSD = 0; // Защита от отрицательных значений
            lpShares -= withdrawnLpShares;
            if (lpShares < 0) lpShares = 0; // Защита от отрицательных значений
        } else if (event.event === 'Transfer') {
            const usdValue = parseFloat(returnValues.usdValue);
            const lpValue = parseFloat(returnValues.value);

            if (returnValues.from === address) {
                balanceUSD -= usdValue;
                lpShares -= lpValue;
                // const transferLpShares = parseFloat(returnValues.value);
                // const transferUSD = (balanceUSD / lpShares) * transferLpShares;
                // balanceUSD -= transferUSD;
                if (lpShares < 0) lpShares = 0; // Защита от отрицательных значений
                // lpShares -= transferLpShares;
                if (balanceUSD < 0) balanceUSD = 0; // Защита от отрицательных значений
            } else if (returnValues.to === address) {
                if (returnValues.placed !== null && !isNaN(parseFloat(returnValues.placed)) && parseFloat(returnValues.placed) !== 0) {
                    balanceUSD += parseFloat(returnValues.placed);
                } else {
                    const transferLpShares = parseFloat(returnValues.value);
                    const transferUSD = (balanceUSD / lpShares) * transferLpShares;
                    balanceUSD += transferUSD;
                }
                lpShares += lpValue;
            }
            //console.log(`Updated balanceUSD : ${balanceUSD}, lpShares : ${lpShares}`);
        }
    }

    if (lpShares === 0) {
        console.log(`No lpShares available for address : ${address}`);
        return 0;
    }
    
    const usdValue = (balanceUSD / lpShares) * parseFloat(event.returnValues.value) / Math.pow(10, 18); 
    console.log(`Calculated USD value for Transfer event : ${event.transactionHash} is ${usdValue}`);
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
        logError(`\nFailed to calculate USD value for Transfer event : ${transactionHash}, error : ${error}`);
        res.status(500).send('\nFailed to calculate USD value for Transfer event');
    }
});
    
// Новый маршрут для получения всех событий
app.get('/events', async (req, res) => {
    try {
        const [events] = await pool.query('SELECT * FROM contract_events ORDER BY eventDate DESC');
        res.json(events);
    } catch (error) {
        logError(`Failed to fetch contract events : ${error}`);
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
        logError(`Failed to fetch events for wallet ${wallet} : ${error.message}`);
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
        logError(`Failed to extract unique depositors : ${error}`);
        return [];
    } finally {
        if (connection) connection.release();
    }
}

// Функция для заполнения таблицы unique_depositors уникальными адресами
async function populateUniqueDepositors() {
    try {
        console.log(`Populating unique depositors...`);
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
                logError(`Failed to add depositor ${depositor} : ${error}`);
            }
        }
        console.log(`\nFinished populating unique depositors.`);
        console.log(`Total unique depositors : ${existingCount+addedCount}`);
        console.log(`Added new               : ${addedCount}`);
        console.log(`Already existing        : ${existingCount}\n`);
    } catch (error) {
        logError(`Failed to populate unique depositors : ${error}`);
    }
}

// Функция для добавления одного уникального адреса в таблицу unique_depositors
async function addUniqueDepositor(depositor) {
    try {
        const [result] = await pool.query(
            `INSERT INTO unique_depositors (depositor_address)
             VALUES (?)
             ON DUPLICATE KEY UPDATE depositor_address = depositor_address`,
            [depositor]
        );

        // Если строка была добавлена (т.е. нового адреса не было)
        if (result.affectedRows > 0 && result.warningCount === 0) {
            console.log(`Added new depositor: ${depositor}`);
        } else {
            console.log(`Depositor already exists: ${depositor}`);
        }
    } catch (error) {
        console.error(`Failed to add depositor ${depositor}: ${error.message}`);
    }
}

// Маршрут для получения всех уникальных адресов
app.get('/depositors', async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM unique_depositors ORDER BY id DESC');
        res.json(rows);
    } catch (error) {
        logError(`Failed to fetch unique depositors : ${error}`);
        res.status(500).send('Failed to fetch unique depositors');
    }
});

// Расчет дохода DSF через получение USDT на адрес стратегии, а также реальныхсумм при выводе пользователей

// Функция для получения всех транзакций DAI по указанному адресу
async function getDAITransactions(address) {
    try {    
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
    } catch (error) {
        logError(`Failed to get DAI transactions for address ${address} : ${error.message}`);
        return [];
    }
}

// Функция для получения всех транзакций USDC по указанному адресу
async function getUSDCTransactions(address) {
    try {    
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
    } catch (error) {
        logError(`Failed to get USDС transactions for address ${address} : ${error.message}`);
        return [];
    }
}

// Функция для получения всех транзакций USDT по указанному адресу
async function getUSDTTransactions(address) {
    try {        
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
    } catch (error) {
        logError(`Failed to get USDT transactions for address ${address} : ${error.message}`);
        return [];
    }
}

// Функция для получения пополнений и выводов DAI по указанному блоку для указанного адреса
async function getDAITransactionsByBlock(address, blockNumber) {

    try {
        const transactions = await getDAITransactions(address);
        const filteredTransactions = transactions.filter(tx => tx.blockNumber === blockNumber);

        const deposits = filteredTransactions.filter(tx => tx.to.toLowerCase() === address.toLowerCase());
        const withdrawals = filteredTransactions.filter(tx => tx.from.toLowerCase() === address.toLowerCase());

        const totalDeposits = deposits.reduce((sum, tx) => sum + Number(formatBigInt(tx.value, 18)), 0);
        const totalWithdrawals = withdrawals.reduce((sum, tx) => sum + Number(formatBigInt(tx.value, 18)), 0);

        const balanceDifference = totalDeposits - totalWithdrawals;

        return balanceDifference;
    } catch (error) {
        logError(`Failed to get DAI transactions for address ${address} in block ${blockNumber} : ${error}`);
        throw new Error('Failed to get DAI transactions');
    }
}

// Функция для получения пополнений и выводов USDC по указанному блоку для указанного адреса
async function getUSDCTransactionsByBlock(address, blockNumber) {

    try {
        const transactions = await getUSDCTransactions(address);
        const filteredTransactions = transactions.filter(tx => tx.blockNumber === blockNumber);

        const deposits = filteredTransactions.filter(tx => tx.to.toLowerCase() === address.toLowerCase());
        const withdrawals = filteredTransactions.filter(tx => tx.from.toLowerCase() === address.toLowerCase());

        const totalDeposits = deposits.reduce((sum, tx) => sum + Number(formatBigInt(tx.value, 6)), 0);
        const totalWithdrawals = withdrawals.reduce((sum, tx) => sum + Number(formatBigInt(tx.value, 6)), 0);

        const balanceDifference = totalDeposits - totalWithdrawals;

        return balanceDifference;
    } catch (error) {
        logError(`Failed to get USDT transactions for address ${address} in block ${blockNumber} : ${error}`);
        throw new Error('Failed to get USDT transactions');
    }
}

// Функция для получения пополнений и выводов USDT по указанному блоку для указанного адреса
async function getUSDTTransactionsByBlock(address, blockNumber) {

    try {
        const transactions = await getUSDTTransactions(address);
        const filteredTransactions = transactions.filter(tx => tx.blockNumber === blockNumber);

        const deposits = filteredTransactions.filter(tx => tx.to.toLowerCase() === address.toLowerCase());
        const withdrawals = filteredTransactions.filter(tx => tx.from.toLowerCase() === address.toLowerCase());

        const totalDeposits = deposits.reduce((sum, tx) => sum + Number(formatBigInt(tx.value, 6)), 0);
        const totalWithdrawals = withdrawals.reduce((sum, tx) => sum + Number(formatBigInt(tx.value, 6)), 0);

        const balanceDifference = totalDeposits - totalWithdrawals;

        return balanceDifference;
    } catch (error) {
        logError(`Failed to get USDT transactions for address ${address} in block ${blockNumber} : ${error}`);
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
            //console.log(` -  - Balance Difference : ${balanceDifference}`);
            totalBalanceDifference += Number(balanceDifference);
        } catch (error) {
            logError(`Failed to get balance difference for address ${address} in block ${blockNumber} : ${error}`);
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
        logError(`${error}`);
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
    try {
        const response = await retryEtherscan(async () => {
            const apiKey = getNextEtherscanApiKey();
            const url = `https://api.etherscan.io/api?module=account&action=txlist&address=${address}&startblock=${blockNumber}&endblock=${blockNumber}&sort=asc&apikey=${apiKey}`;
            
            console.log(url)
            
            return await axios.get(url);
        });

        //console.log(`response`,response);

        if (response.data.status === '1' && response.data.message === 'OK') {
            return response.data.result;
        } else {
            console.warn(`No transactions found for address ${address} in block ${blockNumber}`);
            return [];
        }
    } catch (error) {
        logError(`Failed to get transactions for address ${address} in block ${blockNumber} : ${error.message}`);
        return [];
    }
}

// Функция для получения статуса транзакции 'Optimized' или 'Standard' при наличии адреса DSF транзакция будет 'Optimized'
async function showBlockTransactions(address, blockNumber) {
    try {

        logWarning(`Checking transactions for address ${address} in block ${blockNumber}`);

        // Получаем все транзакции для данного адреса и блока
        const transactions = await getTransactionsByAddressAndBlock(address, blockNumber);

        // Возвращаем 'Optimized', если есть транзакции, иначе 'Standard'
        return transactions.length > 0 ? 'Optimized' : 'Standard';
    } catch (error) {
        logError(`Failed to get transactions for address ${address} in block ${blockNumber} : ${error}`);
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
        console.log(`Transactions in block ${blockNumber} : ${transactions}`);
        res.json(transactions);
    } catch (error) {
        logError(`${error}`);
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
        // Получение всех событий `AutoCompoundAll` и вычисление TotalIncomeDSF и AutoCompoundAllCostUsd
        const [autoCompoundAllEvents] = await pool.query(`
            SELECT SUM(CAST(returnValues->'$.incomeDSF' AS DECIMAL(18, 8))) AS TotalIncomeDSF, SUM(transactionCostUsd) AS AutoCompoundAllCostUsd
            FROM contract_events
            WHERE event = 'AutoCompoundAll'
        `);

        // Получение всех событий `ClaimedAllManagementFee` и вычисление ClaimedAllManagementFeeCostUsd
        const [claimedAllManagementFeeEvents] = await pool.query(`
            SELECT SUM(transactionCostUsd) AS ClaimedAllManagementFeeCostUsd
            FROM contract_events
            WHERE event = 'ClaimedAllManagementFee'
        `);

        // Получение всех уникальных событий `Deposited` с `transaction_status` : `Optimized` и вычисление DepositedCostUsd
        const [depositedEvents] = await pool.query(`
            SELECT SUM(transactionCostUsd) AS DepositedCostUsd
            FROM (
                SELECT DISTINCT blockNumber, transactionCostUsd
                FROM contract_events
                WHERE event = 'Deposited' AND JSON_UNQUOTE(returnValues->'$.transaction_status') = 'Optimized'
            ) AS uniqueDepositedEvents
        `);

        // Получение всех уникальных событий `Withdrawn` с `transaction_status` : `Optimized` и вычисление WithdrawnCostUsd
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
        logError(`Failed to fetch event summary : ${error}`);
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
        logError(`Failed to fetch monthly event summary : ${error}`);
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
                // console.log(`Deposited : ${shares} shares to ${returnValues.depositor}, totalLpShares : ${totalLpShares}`);
            } else if (event.event === 'Withdrawn') {
                const shares = parseFloat(returnValues.lpShares || 0);

                totalLpShares -= shares;
                lpShares.set(returnValues.withdrawer, (lpShares.get(returnValues.withdrawer) || 0.0) - shares);
                // console.log(`Withdrawn : ${shares} shares from ${returnValues.withdrawer}, totalLpShares : ${totalLpShares}`);
            } else if (event.event === 'Transfer') {
                const shares = parseFloat(returnValues.value || 0);

                lpShares.set(returnValues.from, (lpShares.get(returnValues.from) || 0.0) - shares);
                lpShares.set(returnValues.to, (lpShares.get(returnValues.to) || 0.0) + shares);
                // console.log(`Transfer : ${shares} shares from ${returnValues.from} to ${returnValues.to}`);
            } else if (event.event === 'AutoCompoundAll') {
                const incomeDSF = parseFloat(returnValues.incomeDSF || 0);
                // logWarning(`AutoCompoundAll : incomeDSF ${incomeDSF} at block ${event.blockNumber}`);

                for (const wallet of uniqueDepositors) {
                    const walletLpShares = lpShares.get(wallet) || 0.0;
                    const walletIncomeBefore = incomeDSFMap.get(wallet) || 0.0;
                    
                    if (walletLpShares > 0.0) {
                        const walletShare = walletLpShares / totalLpShares;
                        const walletIncome = (incomeDSF * walletShare);

                        // Суммируем текущий walletIncome с предыдущими
                        incomeDSFMap.set(wallet, walletIncomeBefore + walletIncome);

                        //incomeDSFMap.set(wallet, (incomeDSFMap.get(wallet) || 0.0) + walletIncome);
                        // console.log(`AutoCompoundAll : wallet ${wallet}, walletIncome ${walletIncome}, totalWalletIncome ${incomeDSFMap.get(wallet)}`);
                    } else {
                        // console.log(`AutoCompoundAll : wallet ${wallet}, walletIncome 0.0, totalWalletIncome ${incomeDSFMap.get(wallet)}`);
                    }
                }
            }
        } catch (error) {
            logError(`Failed to process event ${event.id} : ${error.message}`);
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
        console.log(`Stored all incomeDSF with result : ${JSON.stringify(result)}`);
    } catch (error) {
        logError(`Failed to store all incomeDSF : ${error.message}`);
    }
}

// Эндпоинт для получения всех данных доходов DSF с каждого кошелька 
app.get('/api/incomDSFfromEveryOne', async (req, res) => {
    try {
        const query = `SELECT * FROM incomDSFfromEveryOne`;
        const [rows] = await pool.query(query);
        console.log(`Fetched incomDSFfromEveryOne data : ${JSON.stringify(rows)}`);
        res.json(rows);
    } catch (error) {
        logError(`Failed to fetch incomDSFfromEveryOne data : ${error.message}`);
        res.status(500).send('Failed to fetch data');
    }
});

// Эндпоинт для получения данных доходов DSF с конкретного кошелька 
app.get('/api/incomDSFfromEveryOne/:wallet', async (req, res) => {
    const { wallet } = req.params;
    try {
        const query = `SELECT * FROM incomDSFfromEveryOne WHERE wallet_address = ?`;
        const [rows] = await pool.query(query, [wallet]);
        console.log(`Fetched incomDSFfromEveryOne data for wallet ${wallet} : ${JSON.stringify(rows)}`);
        res.json(rows[0] || {});
    } catch (error) {
        logError(`Failed to fetch incomDSFfromEveryOne data for wallet ${wallet} : ${error.message}`);
        res.status(500).send('Failed to fetch data');
    }
});


//
//
// API keys
//
//

const apiKeys = new Set([
    process.env.API_KEY_1,
    process.env.API_KEY_2,
    process.env.API_KEY_3,
    process.env.API_KEY_4,
    process.env.API_KEY_5,
    process.env.API_KEY_6,
    process.env.API_KEY_7,
    process.env.API_KEY_8,
    process.env.API_KEY_9,
    process.env.API_KEY_10
]);

// const checkApiKey = (req, res, next) => {
//     const apiKey = req.query.apikey;
//     if (apiKeys.has(apiKey)) {
//         next();
//     } else {
//         res.status(403).json({ error: 'Invalid API key' });
//     }
// };

// Middleware для проверки API ключа
const checkApiKey = (req, res, next) => {
    const apiKey = req.query.apikey;
    if (!apiKey || !apiKeys.has(apiKey)) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    next();
};

// Эндпоинт для получения событий, связанных с конкретным кошельком
// https://api2.dsf.finance/request/events?wallet=0xYourWalletAddress&apikey=your_valid_api_key_here
app.get('/request/events', checkApiKey, async (req, res) => {
    const { wallet } = req.query;
    
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

// Эндпоинт для получения данных доходов DSF с конкретного кошелька
// https://api2.dsf.finance/request/incomDSFfromEveryOne?wallet=0xYourWalletAddress&apikey=your_valid_api_key_here
app.get('/request/incomDSFfromEveryOne', checkApiKey, async (req, res) => {
    const { wallet } = req.query;
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

// Эндпоинт для получения всех данных для конкретного кошелька
// https://api2.dsf.finance/request/walletinfo?wallet=0xYourWalletAddress&apikey=your_valid_api_key_here
app.get('/request/walletinfo', checkApiKey, async (req, res) => {
    try {
        const { wallet } = req.query;
        if (!wallet) {
            return res.status(400).json({ error: 'Missing wallet parameter' });
        }

        const walletAddress_ = wallet.toLowerCase();
        const walletAddress = normalizeAddress(walletAddress_);

        const apyToday = await getApyToday().catch(() => 0);

        // Если адрес некорректный, возвращаем значения по умолчанию
        if (!walletAddress) {
            logError('Invalid wallet address:', walletAddress_);
            return res.json(getDefaultWalletData(0, 0, 0, 0, apyToday));
        }

        console.log(`\nWallet Address          :`, walletAddress);

        if (!/^(0x)?[0-9a-f]{40}$/i.test(walletAddress)) {
            logError(`Адрес не соответствует ожидаемому формату.`);
            return res.status(400).json({ error: 'Invalid wallet address format' });
        } else {
            logSuccess(`Адрес соответствует ожидаемому формату.`);
        }

        let connection;
        try {
            connection = await pool.getConnection();

            // Проверяем наличие кошелька в базе данных wallet_info
            const [walletRows] = await connection.query('SELECT * FROM wallet_info WHERE wallet_address = ?', [walletAddress]);
            console.log(`Wallet info for ${walletAddress}: ${walletRows.length}`);

            if (walletRows.length === 0) {
                // Если кошелек не найден в wallet_info, возвращаем ошибку
                logWarning(`Wallet ${walletAddress} not found in wallet_info.`);
                return res.json(getDefaultWalletData(0, 0, 0, 0, apyToday));
            } else {
                // Если данные уже есть, возвращаем их
                console.log(`Данные кошелька уже есть`);
                res.json(walletRows[0]);
            }
        } catch (error) {
            // Обработка ошибок при соединении или выполнении SQL-запроса
            logError(`Database connection or operation failed : ${error}`);
            res.status(500).send('Internal Server Error');
        } finally {
            // Освобождение соединения
            if (connection) connection.release();
        }
    } catch (error) {
        logError(`Unexpected error: ${error}`);
        res.status(500).send('Internal Server Error');
    }
});


//
//
// Эксперементальный раздел
//
//

// Транзакция по адресу и блоку
app.get('/transactions/:address/:blockNumber', async (req, res) => {
    const { address, blockNumber } = req.params;

    try {
        const transactions = await getTransactionsByAddressAndBlock(address, blockNumber);

        res.json(transactions);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});


const CHART_URL = 'https://yields.llama.fi/chart/8a20c472-142c-4442-b724-40f2183c073e';

// АПИ из ДефиЛАМА
app.get('/yields_llama', async (req, res) => {
    try {
        // Проверяем наличие данных в кэше
        const cachedData = llama.get('chartData');
        if (cachedData) {
            console.log('Returning cached data');
            return res.json(cachedData);
        }

        // Данные не найдены в кэше, выполняем запрос
        const response = await axios.get(CHART_URL);
        const chartData = response.data;

        // Сохраняем данные в кэш
        llama.set('chartData', chartData);
        console.log('Data fetched and cached');

        // Возвращаем данные клиенту
        res.json(chartData);
    } catch (error) {
        console.error('Error fetching chart data:', error.message);
        res.status(500).json({ error: 'Failed to fetch chart data' });
    }
});

// Пересылка АПИ Стаса для показа транзакция в дАпп
app.get('/api/deposit/all/:address', async (req, res) => {
    const { address } = req.params;

    try {
        // Выполняем запрос к внешнему API
        const response = await axios.get(`https://api.dsf.finance/deposit/all/${address}`);

        // Пересылаем полученный результат клиенту
        res.json(response.data);
    } catch (error) {
        console.error(`Failed to fetch deposit data for address ${address}: ${error.message}`);

        // Если произошла ошибка, возвращаем код ошибки и сообщение
        res.status(500).json({
            error: 'Failed to fetch deposit data',
            message: error.message
        });
    }
});

//
//
// Доход DSF реалтайм
//
//

// Эндпоинт для получения заработка проекта
app.get('/earnings', async (req, res) => {
    try {
        // Получаем общие данные параллельно
        const [
            crvEarned,
            cvxTotalCliffs,
            cvx_totalSupply,
            cvx_reductionPerCliff,
            cvx_balanceOf,
            crv_balanceOf
        ] = await Promise.all([
            retry(() => cvxRewardsContract.methods.earned(contractsLib.DSFStrategy).call()),
            retry(() => config_cvxContract.methods.totalCliffs().call()),
            retry(() => config_cvxContract.methods.totalSupply().call()),
            retry(() => config_cvxContract.methods.reductionPerCliff().call()),
            retry(() => config_cvxContract.methods.balanceOf(contractsLib.DSFStrategy).call()),
            retry(() => config_crvContract.methods.balanceOf(contractsLib.DSFStrategy).call())
        ]);

        const crvEarnedBigInt = BigInt(crvEarned);
        const cvxTotalCliffsBigInt = BigInt(cvxTotalCliffs);
        const cvx_totalSupplyBigInt = BigInt(cvx_totalSupply);
        const cvx_reductionPerCliffBigInt = BigInt(cvx_reductionPerCliff);
        const cvx_balanceOfBigInt = BigInt(cvx_balanceOf);
        const crv_balanceOfBigInt = BigInt(crv_balanceOf);
        
        // Выполняем расчеты
        // Выполняем расчеты с использованием BigInt
        const cvxRemainCliffs = cvxTotalCliffsBigInt - cvx_totalSupplyBigInt / cvx_reductionPerCliffBigInt;
        const amountInCVX = (crvEarnedBigInt * cvxRemainCliffs) / cvxTotalCliffsBigInt + cvx_balanceOfBigInt;
        const amountInCRV = crvEarnedBigInt + crv_balanceOfBigInt;

        logInfo(`amountInCVX ${amountInCVX}, amountInCRV ${amountInCRV} `)
        // Получаем стоимость CRV и CVX
        const [crvCostArray, cvxCostArray] = await Promise.all([
            retry(() => routerContract.methods.getAmountsOut(amountInCRV, crvToUsdtPath).call()),
            retry(() => routerContract.methods.getAmountsOut(amountInCVX, cvxToUsdtPath).call())
        ]);
        logInfo(`crvCostArray ${crvCostArray}, cvxCostArray ${cvxCostArray} `)

        const crvCost = Number(crvCostArray[crvCostArray.length - 1]) / 1e6;
        const cvxCost = Number(cvxCostArray[cvxCostArray.length - 1]) / 1e6;

        logInfo(`crvCost ${crvCost}, cvxCost ${cvxCost} `)

        // Подготавливаем данные для ответа
        const earningsData = {
            amountInCRV: Number(amountInCRV) / 1e18,
            amountInCVX: Number(amountInCVX) / 1e18,
            crvCost: crvCost,
            cvxCost: cvxCost,
            total: (crvCost+cvxCost)
        };

        // Возвращаем данные
        res.json(earningsData);
    } catch (error) {
        console.error(`Error fetching project earnings: ${error.message}`);
        res.status(500).json({ error: 'Failed to fetch project earnings' });
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
        // Получение текущей цены ETH в USD
        const price = await getEthUsdPrice(Math.floor(Date.now() / 1000)); // Текущая дата в виде timestamp

        // Установка флага инициализации
        initializationCompleted = false;

        await delay(1000); // Задержка 1 секунда
        // Проверка всех API ключей
        await checkAllApiKeys();

        // Обновляем историю цены ETH
        await initializeEthPriceData();
        await delay(1000); // Задержка 1 секунда

        // Проверка на упущенные Events
        await initializeMissingEvents().catch(console.error);
        await removeDuplicateEvents(); // удаления возможных дублей
        await delay(40000); // Задержка 40 секунда

        // Инициализация таблицы и заполнение уникальными депозиторами
        await populateUniqueDepositors();
        await delay(1000); // Задержка 1 секунда

        // Обновляем кэш при запуске сервера
        await updateAllDeposits();
        await delay(1000); // Задержка 1 секунда

        //await updateApyData();
        //logSuccess(`APY data updated successfully.`);
        
        // Обновление данныхкошельков
        await updateAllWallets();
        //logSuccess(`Wallets updated successfully.`);

        // Расчет доходов DSF с каждого кошелька
        await calculateIncomeDSF();

        initializationTelegramBotEvents = true;
        initializationCompleted = true;
        console.log(`checkForNewEvents : activated (${initializationCompleted})`);

    } catch (error) {
        logError(`Failed to update all data : ${error}`);
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

    setInterval(checkForNewEvents, 30000);  // Проверка каждые 30 секунд
});

// Увеличение таймаута соединения
server.keepAliveTimeout = 120000; // 120 секунд
server.headersTimeout = 121000; // 121 секунда
