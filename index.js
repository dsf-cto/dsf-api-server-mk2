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

dotenv.config();

import contractsLib from './utils/contract_addresses.json' assert {type: 'json'};
import dsfABI from './utils/dsf_abi.json' assert {type: 'json'};
import dsfStrategyABI from './utils/dsf_strategy_abi.json' assert {type: 'json'};
import dsfRatioABI from './utils/dsf_ratio_abi.json' assert {type: 'json'};
import crvRewardsABI from './utils/crv_rewards_abi.json' assert {type: 'json'};
import uniswapRouterABI from './utils/uniswap_router_abi.json' assert {type: 'json'};
import crvABI from './utils/CRV_abi.json' assert {type: 'json'};
import cvxABI from './utils/CVX_abi.json' assert {type: 'json'};

const app = express();

// Ограничение частоты запросов
const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 минут
    max: 100, // Ограничение: 100 запросов с одного IP за 15 минут
    message: "Too many requests from this IP, please try again later."
});

app.use(limiter);

// Настройка CORS
const corsOptions = {
    origin: '*', // Разрешить доступ всем источникам
    // origin: (origin, callback) => {
    //     const allowedOrigins = [
    //         /^https:\/\/(?:.*\.)?dsf\.finance$/,
    //         'https://dsf-dapp-mk2.vercel.app'
    //     ];
    //     if (allowedOrigins.some(pattern => pattern.test(origin))) {
    //         callback(null, true);
    //     } else {
    //         callback(new Error('Not allowed by CORS'));
    //     }
    // },
    methods: ['GET', 'POST', 'PUT', 'DELETE'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: true
};

app.use(cors(corsOptions));

const colors = {
    red: '\x1b[31m',
    yellow: '\x1b[33m',
    green: '\x1b[32m',
    blue: '\x1b[34m',
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

// Middleware to redirect HTTP to HTTPS
app.use((req, res, next) => {
    if (req.headers['x-forwarded-proto'] !== 'https') {
        return res.redirect(`https://${req.headers.host}${req.url}`);
    }
    next();
});

const pool = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// For RESTART DataBase apy_info: 
// const dropTableQuery = `DROP TABLE IF EXISTS apy_info;`;
//         await pool.query(dropTableQuery);
//         console.log('Таблица успешно удалена');

// For RESTART DataBase : 
// const dropTableQuery = `DROP TABLE IF EXISTS wallet_info;`;
//         await pool.query(dropTableQuery);
//         console.log('Таблица успешно удалена');

const createTableQuery = `
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
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
`;

async function initializeDatabase() {
    let connection;
    try {
        connection = await pool.getConnection();
        await connection.query(createTableQuery);
        console.log("Table 'wallet_info' checked/created successfully.");
    } catch (error) {
        logError("Failed to create 'wallet_info' table:", error);
    } finally {
        if (connection) connection.release();
    }
}

initializeDatabase();

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
        logError(`Failed to connect to provider ${providers[providerIndex]}:`, error);
        //Go to the next provider
        providerIndex = (providerIndex + 1) % providers.length;
        await connectToWeb3Provider(); // Recursively try to connect to the next ISP
    }
}

connectToWeb3Provider();

const contractDSF = new web3.eth.Contract(dsfABI, contractsLib.DSFmain);
const contractDSFStrategy = new web3.eth.Contract(dsfStrategyABI, contractsLib.DSFStrategy);
const ratioContract = new web3.eth.Contract(dsfRatioABI, contractsLib.DSFratio);
const cvxRewardsContract = new web3.eth.Contract(crvRewardsABI, contractsLib.crvRewards);
const routerContract = new web3.eth.Contract(uniswapRouterABI, contractsLib.uniswapV2Router);
const config_crvContract = new web3.eth.Contract(crvABI, contractsLib.CRV);
const config_cvxContract = new web3.eth.Contract(cvxABI, contractsLib.CVX);

const crvToUsdtPath = [contractsLib.CRV,contractsLib.WETH,contractsLib.USDT];
const cvxToUsdtPath = [contractsLib.CVX,contractsLib.WETH,contractsLib.USDT];

async function getWalletData(walletAddress_) {
    if (!walletAddress_) {
        throw new Error("\nwalletAddress is not defined");
    }
    const walletAddress = normalizeAddress(walletAddress_);
    console.log('\nNormalized Address     :', walletAddress);

    let ratioUser_ = 0; // Установите значение по умолчанию на случай ошибки

    try {
        ratioUser_ = await ratioContract.methods.calculateLpRatio(walletAddress).call();
        console.log('ratioUser_:',ratioUser_);
    } catch (error) {
        logError("Error occurred while fetching ratio:", error);
        ratioUser_ = 0; // Установка значения 0 в случае ошибки
        console.log('ratioUser_:',ratioUser_);
    }

    if (ratioUser_ === 0) {
        logWarning("userDeposits       USDT: 0");
        logWarning("dsfLpBalance     DSF LP: 0");
        logWarning("ratioUser             %: 0");
        logWarning("availableWithdraw  USDT: 0");
        logWarning("cvxShare            CVX: 0");
        logWarning("cvxCost            USDT: 0");
        logWarning("crvShare            CRV: 0");
        logWarning("crvCost            USDT: 0");
        return {
            userDeposits: 0,
            dsfLpBalance: 0,
            safeRatioUser: 0,
            availableToWithdraw: 0,
            cvxShare: 0,
            cvxCost: 0,
            crvShare: 0,
            crvCost: 0
        };
    }

    let availableToWithdraw_;

    try {
        availableToWithdraw_ = await contractDSFStrategy.methods.calcWithdrawOneCoin(ratioUser_, 2).call();
        console.log('availableToWithdraw_:',availableToWithdraw_);
    } catch (error) {
        logError("Error occurred while fetching available to withdraw:", error);
        availableToWithdraw_ = 0; // Установка значения 0 в случае ошибки
        console.log('availableToWithdraw_:',availableToWithdraw_);
    }

    let dsfLpBalance_;

    try {
        dsfLpBalance_ = await contractDSF.methods.balanceOf(walletAddress).call();
        console.log('dsfLpBalance_:',dsfLpBalance_);
    } catch (error) {
        logError("Error occurred while fetching DSF LP balance:", error);
        dsfLpBalance_ = 0; // Установка значения 0 в случае ошибки
        console.log('dsfLpBalance_:',dsfLpBalance_);
    }

    try {
        const availableToWithdraw = Number(availableToWithdraw_) / 1e6
        const dsfLpBalance = (Number(dsfLpBalance_) / 1e18).toPrecision(18);

        const response = await axios.get(`https://api.dsf.finance/deposit/${walletAddress}`);
        console.log('response:',response.data);
        const userDeposits = Number(response.data.beforeCompound) + Number(response.data.afterCompound); // Сумма значений
        console.log('userDeposits:',userDeposits);
        const crvEarned = await cvxRewardsContract.methods.earned(contractsLib.DSFStrategy).call();
        console.log('crvEarned:',crvEarned);
        const cvxTotalCliffs = await config_cvxContract.methods.totalCliffs().call();
        console.log('cvxTotalCliffs:',cvxTotalCliffs);
        const cvx_totalSupply = await config_cvxContract.methods.totalSupply().call();
        console.log('cvx_totalSupply:',cvx_totalSupply);
        const cvx_reductionPerCliff = await config_cvxContract.methods.reductionPerCliff().call();
        console.log('cvx_reductionPerCliff:',cvx_reductionPerCliff);
        const cvx_balanceOf = await config_cvxContract.methods.balanceOf(contractsLib.DSFStrategy).call();
        console.log('cvx_balanceOf:',cvx_balanceOf);
        const crv_balanceOf = await config_crvContract.methods.balanceOf(contractsLib.DSFStrategy).call();
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
            const crvCost_Array = await routerContract.methods.getAmountsOut(Math.trunc(crvShare_), crvToUsdtPath).call();
            const cvxCost_Array = await routerContract.methods.getAmountsOut(Math.trunc(cvxShare_), cvxToUsdtPath).call();
            console.log('crvCost_Array:',crvCost_Array);
            crvCost = Number(crvCost_Array[crvCost_Array.length - 1]) / 1e6;
            console.log('crvCost:',crvCost);
            cvxCost = Number(cvxCost_Array[cvxCost_Array.length - 1]) / 1e6;
            console.log('cvxCost:',cvxCost);

            crvShare = Number(crvShare_) / 1e18;
            cvxShare = Number(cvxShare_) / 1e18;
            console.log('cvxShare:',cvxShare);
        } 

        const ratioUser = parseFloat(ratioUser_) / 1e16;
        console.log('ratioUser:',ratioUser);
        const safeRatioUser = (ratioUser ? parseFloat(ratioUser) : 0.0).toPrecision(16);
        console.log('safeRatioUser:',safeRatioUser);
        

        //console.log("response               : " + response);
        console.log("userDeposits       USDT: " + userDeposits);
        console.log("dsfLpBalance     DSF LP: " + dsfLpBalance);
        console.log("ratioUser             %: " + safeRatioUser);
        console.log("availableWithdraw  USDT: " + availableToWithdraw);
        console.log("cvxShare            CVX: " + cvxShare);
        console.log("cvxCost            USDT: " + cvxCost);
        console.log("crvShare            CRV: " + crvShare);
        console.log("crvCost            USDT: " + crvCost);

        return {
            userDeposits,
            dsfLpBalance,
            safeRatioUser,
            availableToWithdraw,
            cvxShare,
            cvxCost,
            crvShare,
            crvCost
        };
    } catch (error) {
        logError('Error retrieving data for wallet:', walletAddress, error);
        logWarning("userDeposits       USDT: 0");
        logWarning("dsfLpBalance     DSF LP: 0");
        logWarning("ratioUser             %: 0");
        logWarning("availableWithdraw  USDT: 0");
        logWarning("cvxShare            CVX: 0");
        logWarning("cvxCost            USDT: 0");
        logWarning("crvShare            CRV: 0");
        logWarning("crvCost            USDT: 0");
        return {
            userDeposits: 0,
            dsfLpBalance: 0,
            safeRatioUser: 0,
            availableToWithdraw: 0,
            cvxShare: 0,
            cvxCost: 0,
            crvShare: 0,
            crvCost: 0
        };
    }
}

async function getWalletDataOptim(walletAddress_, cachedData) {
    if (!walletAddress_) {
        throw new Error("\nwalletAddress is not defined");
    }
    const walletAddress = normalizeAddress(walletAddress_);
    console.log('\nNormalized Address     :', walletAddress);

    let ratioUser_ = 0;

    try {
        ratioUser_ = await ratioContract.methods.calculateLpRatio(walletAddress).call();
        //console.log('ratioUser_:', ratioUser_);
    } catch (error) {
        logError("Error occurred while fetching ratio"); //, error);
        ratioUser_ = 0;
        //console.log('ratioUser_:', ratioUser_);
    }

    if (ratioUser_ === 0) {
        logWarning("userDeposits       USDT: 0");
        logWarning("dsfLpBalance     DSF LP: 0");
        logWarning("ratioUser             %: 0");
        logWarning("availableWithdraw  USDT: 0");
        logWarning("cvxShare            CVX: 0");
        logWarning("cvxCost            USDT: 0");
        logWarning("crvShare            CRV: 0");
        logWarning("crvCost            USDT: 0");
        return {
            userDeposits: 0,
            dsfLpBalance: 0,
            safeRatioUser: 0,
            availableToWithdraw: 0,
            cvxShare: 0,
            cvxCost: 0,
            crvShare: 0,
            crvCost: 0
        };
    }

    let availableToWithdraw_;

    try {
        availableToWithdraw_ = await contractDSFStrategy.methods.calcWithdrawOneCoin(ratioUser_, 2).call();
        //console.log('availableToWithdraw_:', availableToWithdraw_);
    } catch (error) {
        logError("Error occurred while fetching available to withdraw"); //, error);
        availableToWithdraw_ = 0;
        //console.log('availableToWithdraw_:', availableToWithdraw_);
    }

    let dsfLpBalance_;

    try {
        dsfLpBalance_ = await contractDSF.methods.balanceOf(walletAddress).call();
        //console.log('dsfLpBalance_:', dsfLpBalance_);
    } catch (error) {
        logError("Error occurred while fetching DSF LP balance"); //, error);
        dsfLpBalance_ = 0;
        //console.log('dsfLpBalance_:', dsfLpBalance_);
    }

    try {
        const availableToWithdraw = Number(availableToWithdraw_) / 1e6;
        const dsfLpBalance = (Number(dsfLpBalance_) / 1e18).toPrecision(18);

        const response = await axios.get(`https://api.dsf.finance/deposit/${walletAddress}`);
        //console.log('response:', response.data);
        const userDeposits = Number(response.data.beforeCompound) + Number(response.data.afterCompound);
        //console.log('userDeposits:', userDeposits);

        let crvShare = 0;
        let cvxShare = 0;
        let crvCost = 0;
        let cvxCost = 0;

        const crvShare_ = Math.trunc(Number(cachedData.amountInCRV) * Number(ratioUser_) / 1e18 * 0.85);
        const cvxShare_ = Math.trunc(Number(cachedData.amountInCVX) * Number(ratioUser_) / 1e18 * 0.85);
        //console.log('crvShare_:', crvShare_);

        if (crvShare_ > 20000 && cvxShare_ > 20000) {
            const crvCost_Array = await routerContract.methods.getAmountsOut(Math.trunc(crvShare_), crvToUsdtPath).call();
            const cvxCost_Array = await routerContract.methods.getAmountsOut(Math.trunc(cvxShare_), cvxToUsdtPath).call();
            //console.log('crvCost_Array:', crvCost_Array);
            crvCost = Number(crvCost_Array[crvCost_Array.length - 1]) / 1e6;
            //console.log('crvCost:', crvCost);
            cvxCost = Number(cvxCost_Array[cvxCost_Array.length - 1]) / 1e6;
            //console.log('cvxCost:', cvxCost);

            crvShare = Number(crvShare_) / 1e18;
            cvxShare = Number(cvxShare_) / 1e18;
            //console.log('cvxShare:', cvxShare);
        }

        const ratioUser = parseFloat(ratioUser_) / 1e16;
        //console.log('ratioUser:', ratioUser);
        const safeRatioUser = (ratioUser ? parseFloat(ratioUser) : 0.0).toPrecision(16);
        //console.log('safeRatioUser:', safeRatioUser);

        //console.log("response               : " + response);
        console.log("userDeposits       USDT: " + userDeposits);
        console.log("dsfLpBalance     DSF LP: " + dsfLpBalance);
        console.log("ratioUser             %: " + safeRatioUser);
        console.log("availableWithdraw  USDT: " + availableToWithdraw);
        console.log("cvxShare            CVX: " + cvxShare);
        console.log("cvxCost            USDT: " + cvxCost);
        console.log("crvShare            CRV: " + crvShare);
        console.log("crvCost            USDT: " + crvCost);
        return {
            userDeposits,
            dsfLpBalance,
            safeRatioUser,
            availableToWithdraw,
            cvxShare,
            cvxCost,
            crvShare,
            crvCost
        };
    } catch (error) {
        logError('Error retrieving data for wallet:', walletAddress, error);
        logWarning("userDeposits       USDT: 0");
        logWarning("dsfLpBalance     DSF LP: 0");
        logWarning("ratioUser             %: 0");
        logWarning("availableWithdraw  USDT: 0");
        logWarning("cvxShare            CVX: 0");
        logWarning("cvxCost            USDT: 0");
        logWarning("crvShare            CRV: 0");
        logWarning("crvCost            USDT: 0");
        return {
            userDeposits: 0,
            dsfLpBalance: 0,
            safeRatioUser: 0,
            availableToWithdraw: 0,
            cvxShare: 0,
            cvxCost: 0,
            crvShare: 0,
            crvCost: 0
        };
    }
}

function normalizeAddress(address) {
    if (web3.utils.isAddress(address)) {
        return web3.utils.toChecksumAddress(address);
    } else {
        throw new Error('Invalid Ethereum address');
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

app.get('/wallet/:walletAddress', async (req, res) => {

    connectToWeb3Provider();

    const walletAddress_ = req.params.walletAddress.toLowerCase();
    
    const walletAddress = normalizeAddress(walletAddress_);
    console.log('\nNormalized Address     :', walletAddress);

    
        if (!/^(0x)?[0-9a-f]{40}$/i.test(walletAddress)) {
            console.log("Адрес не соответствует ожидаемому формату.");
        } else {
            console.log("Адрес соответствует ожидаемому формату.");
        }
    
        
    //let connection;
    //const walletAddress = req.params.walletAddress.toLowerCase();
    let connection;

    connection = await pool.getConnection();
    //console.log(connection);

    try {
        // Получаем соединение с базой данных
        // Проверяем наличие кошелька в базе данных
        const [rows] = await connection.query('SELECT * FROM wallet_info WHERE wallet_address = ?', [walletAddress]);
        console.log("Rows from database:", rows);
        
        if (rows.length === 0) {
            // Если кошелек не найден, получаем данные и сохраняем их
            console.log("Получаем данные кошелька");
            try {
                const walletData = await getWalletData(walletAddress);
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
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
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
                    walletData.crvCost
                ]);
                // Отправляем полученные данные клиенту
                const serializedData = serializeBigints(walletData); // Сериализация данных
                res.json(serializedData); // Отправка сериализованных данных
            } catch (error) {
                // Логируем ошибку и отправляем ответ сервера
                logError('Failed to retrieve or insert wallet data:', error);
                res.status(500).send('Internal Server Error');
            }
        } else {
            // Если данные уже есть, возвращаем их
            console.log("Данные кошелька уже есть");
            res.json(rows[0]);
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

cron.schedule('0 */3 * * *', async () => {
    console.log('Running a task every 3 hours');
    updateAllWallets(); // Вызов функции обновления всех кошельков
});

async function updateWalletData(walletAddress, cachedData) {
    let connection;
    try {
        // Получение данных кошелька
        const walletData = await getWalletDataOptim(walletAddress, cachedData);
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
            walletAddress
        ];

        // Выполнение запроса обновления
        await connection.query(updateQuery, values);
        console.log(`Data updated for wallet: ${walletAddress}`);
    } catch (error) {
        logError(`Error updating wallet data for ${walletAddress}:`, error);
        throw error;
    } finally {
        // Освобождение соединения
        if (connection) connection.release();
    }
}

app.post('/update/:walletAddress', async (req, res) => {
    const walletAddress_ = req.params.walletAddress.toLowerCase();
    if (!walletAddress_) {
        throw new Error("\nwalletAddress is not defined");
    }
    const walletAddress = normalizeAddress(walletAddress_);
    try {
        await updateWalletData(walletAddress);
        res.send({ message: 'Data updated successfully' });
    } catch (error) {
        logError('Failed to update data:', error);
        res.status(500).send('Failed to update wallet data');
    }
});

async function updateAllWallets() {
    let connection;
    try {
        connection = await pool.getConnection();
        const [wallets] = await connection.query('SELECT wallet_address FROM wallet_info');
        console.log(wallets);

        // Получаем общие данные один раз
        const crvEarned = await cvxRewardsContract.methods.earned(contractsLib.DSFStrategy).call();
        const cvxTotalCliffs = await config_cvxContract.methods.totalCliffs().call();
        const cvx_totalSupply = await config_cvxContract.methods.totalSupply().call();
        const cvx_reductionPerCliff = await config_cvxContract.methods.reductionPerCliff().call();
        const cvx_balanceOf = await config_cvxContract.methods.balanceOf(contractsLib.DSFStrategy).call();
        const crv_balanceOf = await config_crvContract.methods.balanceOf(contractsLib.DSFStrategy).call();
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
        console.log('cachedData : ', cachedData);
        for (const wallet of wallets) {
            await updateWalletData(wallet.wallet_address, cachedData);
        }
        logSuccess('\nAll wallet data updated successfully.');
    } catch (error) {
        logError('\nError during initial wallet data update:', error);
    } finally {
        if (connection) connection.release();
    }
}

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
    logWarning(`Server is listening on port ${port}`);
    updateAllWallets();
});

// Увеличение таймаута соединения
server.keepAliveTimeout = 65000; // 65 секунд
