import { EventEmitter } from 'events';
EventEmitter.defaultMaxListeners = 22;

import express from 'express';
import Web3 from 'web3';
import mysql from 'mysql2/promise'; // Используйте это для работы с промисами
import cron from 'node-cron';
import axios from 'axios';

import contractsLib from './utils/contract_addresses.json' assert {type: 'json'};
import dsfABI from './utils/dsf_abi.json' assert {type: 'json'};
import dsfStrategyABI from './utils/dsf_strategy_abi.json' assert {type: 'json'};
import dsfRatioABI from './utils/dsf_ratio_abi.json' assert {type: 'json'};
import crvRewardsABI from './utils/crv_rewards_abi.json' assert {type: 'json'};
import uniswapRouterABI from './utils/uniswap_router_abi.json' assert {type: 'json'};
import crvABI from './utils/CRV_abi.json' assert {type: 'json'};
import cvxABI from './utils/CVX_abi.json' assert {type: 'json'};

const app = express();

const pool = mysql.createPool({
    host: 'tpark720.beget.tech',
    user: 'tpark720_dsf_api',
    password: '241589DSFapi241589',
    database: 'tpark720_dsf_api',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

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

pool.getConnection()
    .then(connection => {
        connection.query(createTableQuery)
            .then(() => {
                console.log("Table 'wallet_info' checked/created successfully.");
                connection.release();
            })
            .catch(err => {
                console.error("Failed to create 'wallet_info' table:", err);
                connection.release();
            });
    })
    .catch(err => {
        console.error("Database connection failed:", err);
    });

const providers = [
    'https://eth-mainnet.g.alchemy.com/v2/l2vP2jWVn5GXemM54j4HgJQMjMkv9llV',
    'https://mainnet.infura.io/v3/f2f27632352546e19b5a27d2f7be2ee1',
    'https://eth-mainnet.g.alchemy.com/v2/wWnKdcZV4LkHCgJdJKiwY7MiDrH6zyJN',
    'https://eth-mainnet.g.alchemy.com/v2/xrVK352FsLCuyBCRhN6YUKVrANu_lAfW',
    'https://mainnet.infura.io/v3/a676cba70f654892b17f7b957b0af2f8'
];

let providerIndex = 0;
let web3;
//const provider = new Web3.providers.HttpProvider('https://mainnet.infura.io/v3/a676cba70f654892b17f7b957b0af2f8');
//const web3 = new Web3(provider);

async function connectToWeb3Provider() {
    try {
        const selectedProvider = providers[providerIndex];
        web3 = new Web3(selectedProvider);
        await web3.eth.net.getId();
        console.log(`Connected to Ethereum network using provider: ${selectedProvider}`);
    } catch (error) {
        console.error(`Failed to connect to provider ${providers[providerIndex]}:`, error);
        // Переходим к следующему провайдеру
        providerIndex = (providerIndex + 1) % providers.length;
        await connectToWeb3Provider(); // Рекурсивно пытаемся подключиться к следующему провайдеру
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
        throw new Error("walletAddress is not defined");
    }
    const walletAddress = normalizeAddress(walletAddress_);
    console.log('Normalized Address:', walletAddress);

    let ratioUser_ = 0; // Установите значение по умолчанию на случай ошибки

    try {
        ratioUser_ = await ratioContract.methods.calculateLpRatio(walletAddress).call();
    } catch (error) {
        console.error("Error occurred while fetching ratio:", error);
        ratioUser_ = 0; // Установка значения 0 в случае ошибки
    }

    let availableToWithdraw_;

    try {
        availableToWithdraw_ = await contractDSFStrategy.methods.calcWithdrawOneCoin(ratioUser_, 2).call();
    } catch (error) {
        console.error("Error occurred while fetching available to withdraw:", error);
        availableToWithdraw_ = 0; // Установка значения 0 в случае ошибки
    }

    let dsfLpBalance_;

    try {
        dsfLpBalance_ = await contractDSF.methods.balanceOf(walletAddress).call();
    } catch (error) {
        console.error("Error occurred while fetching DSF LP balance:", error);
        dsfLpBalance_ = 0; // Установка значения 0 в случае ошибки
    }

    try {
        const availableToWithdraw = Number(availableToWithdraw_) / 1e6
        const dsfLpBalance = (Number(dsfLpBalance_) / 1e18).toPrecision(18);

        const response = await axios.get(`https://api.dsf.finance/deposit/${walletAddress}`);
        const userDeposits = Number(response.data.beforeCompound) + Number(response.data.afterCompound); // Сумма значений
        const crvEarned = await cvxRewardsContract.methods.earned(contractsLib.DSFStrategy).call();
        const cvxTotalCliffs = await config_cvxContract.methods.totalCliffs().call();
        const cvx_totalSupply = await config_cvxContract.methods.totalSupply().call();
        const cvx_reductionPerCliff = await config_cvxContract.methods.reductionPerCliff().call();
        const cvx_balanceOf = await config_cvxContract.methods.balanceOf(contractsLib.DSFStrategy).call();
        const crv_balanceOf = await config_crvContract.methods.balanceOf(contractsLib.DSFStrategy).call();
        const cvxRemainCliffs = cvxTotalCliffs - cvx_totalSupply / cvx_reductionPerCliff;
        const amountInCVX = (crvEarned * cvxRemainCliffs) / cvxTotalCliffs + cvx_balanceOf;
        const amountInCRV = crvEarned + crv_balanceOf;
        
        let crvShare = 0;
        let cvxShare = 0;
        let crvCost = 0;
        let cvxCost = 0;

        if (ratioUser_ > 0) {
            const crvShare_ = Number(amountInCRV) * Number(ratioUser_) / 1e18 * 0.85; // ratioUser_ CRV
            const cvxShare_ = Number(amountInCVX) * Number(ratioUser_) / 1e18 * 0.85; // ratioUser_ CVX
            
            const crvCost_Array = await routerContract.methods.getAmountsOut(Math.trunc(crvShare_), crvToUsdtPath).call();
            const cvxCost_Array = await routerContract.methods.getAmountsOut(Math.trunc(cvxShare_), cvxToUsdtPath).call();
            
            crvCost = Number(crvCost_Array[crvCost_Array.length - 1]) / 1e6;
            cvxCost = Number(cvxCost_Array[cvxCost_Array.length - 1]) / 1e6;

            crvShare = Number(crvShare_) / 1e18;
            cvxShare = Number(cvxShare_) / 1e18;
        } 

        const ratioUser = parseFloat(ratioUser_) / 1e16;
        console.log('ratioUser:',ratioUser);
        const safeRatioUser = (ratioUser ? parseFloat(ratioUser) : 0.0).toPrecision(16);
        console.log('safeRatioUser:',safeRatioUser);

        console.log("response            : " + response);
        console.log("userDeposits        : " + userDeposits);
        console.log("dsfLpBalance        : " + dsfLpBalance);
        console.log("ratioUser           : " + safeRatioUser + " " + ratioUser_);
        console.log("availableToWithdraw : " + availableToWithdraw);
        console.log("cvxShare            : " + cvxShare);
        console.log("cvxCost in USDT     : " + cvxCost);
        console.log("crvShare            : " + crvShare);
        console.log("crvCost in USDT     : " + crvCost);

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
        console.error('Error retrieving data for wallet:', walletAddress, error);
        ratioUser_ = 0; 
        throw error; // Проброс ошибки для дальнейшей обработки
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
    const walletAddress_ = req.params.walletAddress.toLowerCase();
    const walletAddress = normalizeAddress(walletAddress_);
        console.log('Normalized Address:', walletAddress);
        
    let connection;
    //const walletAddress = req.params.walletAddress.toLowerCase();
    //let connection;

    try {
        // Получаем соединение с базой данных
        connection = await pool.getConnection();
        console.log(connection);
        
        // Проверяем наличие кошелька в базе данных
        const [rows] = await connection.query('SELECT * FROM wallet_info WHERE wallet_address = ?', [walletAddress]);
        console.log("Rows from database:", rows);
        
        if (rows.length === 0) {
            // Если кошелек не найден, получаем данные и сохраняем их
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
                console.error('Failed to retrieve or insert wallet data:', error);
                res.status(500).send('Internal Server Error');
            }
        } else {
            // Если данные уже есть, возвращаем их
            res.json(rows[0]);
        }
    } catch (error) {
        // Обработка ошибок при соединении или выполнении SQL-запроса
        console.error('Database connection or operation failed:', error);
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

async function updateWalletData(walletAddress) {
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
        console.error(`Error updating wallet data for ${walletAddress}:`, error);
        throw error;
    } finally {
        // Освобождение соединения
        if (connection) connection.release();
    }
}

app.post('/api/update/:walletAddress', async (req, res) => {
    const walletAddress = req.params.walletAddress.toLowerCase();
    try {
        await updateWalletData(walletAddress);
        res.send({ message: 'Data updated successfully' });
    } catch (error) {
        console.error('Failed to update data:', error);
        res.status(500).send('Failed to update wallet data');
    }
});

async function updateAllWallets() {
    let connection;
    try {
        connection = await pool.getConnection();
        const [wallets] = await connection.query('SELECT wallet_address FROM wallet_info');
        console.log(wallets);
        for (const wallet of wallets) {
            await updateWalletData(wallet.wallet_address);
        }
        console.log('All wallet data updated successfully.');
    } catch (error) {
        console.error('Error during initial wallet data update:', error);
    } finally {
        if (connection) connection.release();
    }
}

app.get('/add/:walletAddress', async (req, res) => {
    const walletAddress = req.params.walletAddress.toLowerCase();
    let connection;

    try {
        // Получаем соединение с базой данных
        connection = await pool.getConnection();
        
        // Проверяем наличие кошелька в базе данных
        const [rows] = await connection.query('SELECT * FROM wallet_info WHERE wallet_address = ?', [walletAddress]);

        if (rows.length === 0) {
            // Если кошелек не найден, получаем данные и сохраняем их
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
                console.error('Failed to retrieve or insert wallet data:', error);
                res.status(500).send('Internal Server Error');
            }
        } else {
            // Если данные уже есть, возвращаем их
            res.json(rows[0]);
        }
    } catch (error) {
        // Обработка ошибок при соединении или выполнении SQL-запроса
        console.error('Database connection or operation failed:', error);
        res.status(500).send('Internal Server Error');
    } finally {
        // Освобождение соединения
        if (connection) {
            connection.release();
        }
    }
});

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
        console.error('Database connection or operation failed:', error);
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
  console.log(`Server is listening on port ${port}`);
  updateAllWallets();
});

// Увеличение таймаута соединения
server.keepAliveTimeout = 65000; // 65 секунд
