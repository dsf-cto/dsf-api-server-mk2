// import { Telegraf } from 'telegraf';

// const key = '7058666957:AAEaixTBtC50Xd0qDAUjeLyLryINvhjEc0c';
// const chatId = '-1002158927402';
// const bot = new Telegraf(key);

// bot.start((ctx) => {
//     ctx.reply('Салам! Ща буду скидывать сюда инфу по транзакциям.');
// });

// bot.on('text', (ctx) => {
//     const text = ctx.message.text.toLowerCase();
//     if (text.includes('привет')) {
//         ctx.reply('Привет! Ты зачем сюда пишешь??');
//     } else {
//         ctx.reply('Я бот, не надо мне писать, я смотрю блокчейн, не отвлекай меня.');
//     }
// });

// bot.launch();

// export function sendMessageToChat(message) {
//     bot.telegram.sendMessage(chatId, message)
//         .then(() => console.log('Message sent:', message))
//         .catch((error) => console.error('Ошибка при отправке сообщения:', error));
// }

export function sendMessageToChat(message) {
    console.log('bot.telegram');
}

