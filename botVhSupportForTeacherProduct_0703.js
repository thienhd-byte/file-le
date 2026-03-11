const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');
const si = require('systeminformation');
const config = require('config');
const { spawn } = require("child_process");
// replace the value below with the Telegram token you receive from @BotFather
const token = config.external.token_telegram_chatbot;
//const { fetchCpu, fetchRam, fetchServices, fetchWeb, fetchNetworkConnections, fetchProcesses, fetchFsSize } = require('../components/healthCheck/healthService');
const { readFileAndTextToUserProfile } = require('../components/userProfile/userProfileService')
const { Sequelize, QueryTypes } = require('sequelize');
const { Op } = Sequelize;
import Axios from "axios";
const models = require('../../database/models');

let processing = false;
let globalBotInstance = null;

let dataInit = {
    alertUsername: {}
}

// Chỉ cho phép admin chạy lệnh broadcast
const allowedBroadcastSenderIds = new Set(['151333233', 151333233, '6696592763', 6696592763, '5083898895', 5083898895]);

// ===== Album collector cho broadcast nhiều ảnh =====
// Dùng throttleByKey (giống throttleImageForGroup) để gom toàn bộ msg của album
// trước khi xử lý — tránh race condition với await downloadFileFromMessageTele
const pendingBroadcastAlbums = {}; // dùng để đánh dấu media_group_id đang là broadcast

const throttleBroadcastAlbum = throttleByKey(async (...msgs) => {
    // msgs = tất cả msg cùng media_group_id, đã gom đủ sau delay
    const firstMsg = msgs.find(m => m.caption);
    if (!firstMsg) return; // không có caption lệnh → bỏ qua

    const mediaGroupId = firstMsg.media_group_id;
    delete pendingBroadcastAlbums[mediaGroupId]; // dọn dẹp flag

    // Lấy file_id ảnh chất lượng cao nhất từ mỗi msg
    const photos = msgs
        .filter(m => m.photo && m.photo.length)
        .map(m => [...m.photo].pop().file_id);

    // Gọi hàm xử lý broadcast thực sự
    await executeBroadcast(firstMsg, photos);
}, 1500);
// ====================================================

function normalizeGroupCode(groupCode = '') {
    return String(groupCode || '').trim();
}

function normalizeTeacherCode(teacherCode = '') {
    return String(teacherCode || '').trim();
}

function escapeHtml(text = '') {
    return String(text)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

function entityToHtmlTags(entity = {}, fullText = '') {
    const type = entity.type;
    if (type === 'bold') return { open: '<b>', close: '</b>' };
    if (type === 'italic') return { open: '<i>', close: '</i>' };
    if (type === 'underline') return { open: '<u>', close: '</u>' };
    if (type === 'strikethrough') return { open: '<s>', close: '</s>' };
    if (type === 'code') return { open: '<code>', close: '</code>' };
    if (type === 'pre') return { open: '<pre>', close: '</pre>' };
    if (type === 'text_link' && entity.url) return { open: `<a href="${escapeHtml(entity.url)}">`, close: '</a>' };
    if (type === 'text_mention' && entity.user && entity.user.id) return { open: `<a href="tg://user?id=${entity.user.id}">`, close: '</a>' };
    if (type === 'url') {
        const urlText = fullText.slice(entity.offset, entity.offset + entity.length);
        return { open: `<a href="${escapeHtml(urlText)}">`, close: '</a>' };
    }
    if (type === 'mention') {
        const mentionText = fullText.slice(entity.offset, entity.offset + entity.length);
        const username = mentionText.replace('@', '');
        return { open: `<a href="https://t.me/${escapeHtml(username)}">`, close: '</a>' };
    }
    return null;
}

function renderTelegramEntitiesToHtml(text = '', entities = []) {
    const safeText = String(text || '');
    if (!safeText) return '';
    if (!Array.isArray(entities) || entities.length === 0) return escapeHtml(safeText);
    const openAt = Array(safeText.length + 1).fill(null).map(() => []);
    const closeAt = Array(safeText.length + 1).fill(null).map(() => []);
    for (const entity of entities) {
        if (!entity || typeof entity.offset !== 'number' || typeof entity.length !== 'number') continue;
        const start = entity.offset;
        const end = entity.offset + entity.length;
        if (start < 0 || end > safeText.length || end <= start) continue;
        const tags = entityToHtmlTags(entity, safeText);
        if (!tags) continue;
        openAt[start].push({ tag: tags.open, len: entity.length });
        closeAt[end].push({ tag: tags.close, len: entity.length });
    }
    // đóng tag trước, mở tag sau; close sort theo len tăng dần (đóng inner trước), open sort theo len giảm dần (mở outer trước)
    for (let i = 0; i < closeAt.length; i++) closeAt[i].sort((a, b) => a.len - b.len);
    for (let i = 0; i < openAt.length; i++) openAt[i].sort((a, b) => b.len - a.len);
    let out = '';
    for (let i = 0; i < safeText.length; i++) {
        if (closeAt[i] && closeAt[i].length) out += closeAt[i].map(t => t.tag).join('');
        if (openAt[i] && openAt[i].length) out += openAt[i].map(t => t.tag).join('');
        out += escapeHtml(safeText[i]);
    }
    if (closeAt[safeText.length] && closeAt[safeText.length].length) out += closeAt[safeText.length].map(t => t.tag).join('');
    return out;
}

function parseBroadcastCommand(text = '') {
    const raw = String(text || '').trim();
    if (!raw) return null;
    // Hỗ trợ dấu nháy: '...' hoặc “...” hoặc "..."
    const regex = /^Gửi\s+tin\s+nhắn\s+(?:'([^']+)'|"([^"]+)"|“([^”]+)”)\s+cho\s+nhóm\s+(.+)$/i;
    const match = raw.match(regex);
    if (!match) return null;
    const messageText = (match[1] || match[2] || match[3] || '').trim();
    const groupCode = normalizeGroupCode(match[4] || '');
    if (!messageText || !groupCode) return null;
    // Tìm vị trí message trong raw để cắt entities chính xác
    const messageIndex = raw.indexOf(messageText);
    if (messageIndex < 0) return { messageText, groupCode, messageStart: null, messageEnd: null };
    return { messageText, groupCode, messageStart: messageIndex, messageEnd: messageIndex + messageText.length };
}

function sliceEntitiesForSubstring(entities = [], start = 0, end = 0) {
    if (!Array.isArray(entities) || !entities.length) return [];
    const sliced = [];
    for (const entity of entities) {
        if (!entity || typeof entity.offset !== 'number' || typeof entity.length !== 'number') continue;
        const eStart = entity.offset;
        const eEnd = entity.offset + entity.length;
        // chỉ lấy entity nằm hoàn toàn trong đoạn message để tránh phá HTML
        if (eStart >= start && eEnd <= end) {
            sliced.push({ ...entity, offset: eStart - start });
        }
    }
    return sliced;
}
// Template 1: Caption HTML
const caption = `
💌 <b><i>Bot Vận hành gia sư Rino xin chào GV</i></b>,

🔖 GV vui lòng làm theo hướng dẫn dưới đây để kết nối với BOT:

B1: Truy cập vào trang <a href="https://erp.rinoedu.ai/user">https://erp.rinoedu.ai/user</a>.

B2: Copy 6 số "Mã đăng ký Telegram" và gửi vào đây để bắt đầu.

⚠️ Nếu không thể kết nối với BOT, GV liên hệ chuyên viên Quản lý chất lượng để được hỗ trợ.

<i>BOT Vận hành gia sư Rino trân trọng cảm ơn!</i>
———————————————————

💌 Hello Teacher, this is the Rino Tutor Operation Bot.

🔖 Please follow the instructions below to connect with the BOT:

Step 1: Visit <a href="https://erp.rinoedu.ai/user">https://erp.rinoedu.ai/user</a>.

Step 2: Copy the 6-digit “<u>Mã đăng ký Telegram</u>" and send it here to get started.

⚠️ If you are unable to connect to the BOT, please contact the Quality Management Specialist for support.

<i>Thank you sincerely,</i>
<i>Rino Tutor Operation Bot</i>
`;
// Template 2: Caption HTML
const caption2 = `
Xin chào "Username",
BOT Vận hành gia sư Rino sẵn sàng hỗ trợ GV.

✅<b>GV hãy nhắn BOT khi:</b>
📍GV xin nghỉ đột xuất.
📍GV/HS gặp lỗi kỹ thuật.
📍GV báo <b>hủy 10'</b> HS không vào lớp đối với lớp <b>25 phút</b>.
📍GV báo <b>hủy 20'</b> HS không vào lớp đối với lớp <b>trên 25 phút</b>.

⚠️<i>GV lưu ý: VH chỉ hỗ trợ khi tin nhắn đúng cú pháp "<b>Mã lớp + vấn đề cần hỗ trợ</b>" trong cùng <b>MỘT</b> tin nhắn.</i>

<i>Trân trọng,</i>
————

Hello "Username",
The Rino Tutor Operation BOT is ready to support you.

✅ <b>Please message the BOT when you need to:</b>
📍 Request an unexpected leave.
📍 Report technical issues for Teacher/Student.
📍 Report class cancellation when the student does not enter class after <b>10 minutes</b>, applicable to classes with a duration of 25 minutes.
📍 Report class cancellation when the student does not enter class after <b>20 minutes</b>, applicable to classes over 25 minutes in duration.

⚠️ <i><b>Please note</b>: The Operations Team can only support requests that follow the correct format - “<b>Class Code + Issue</b>” - sent in <b>ONE</b> single message.</i>

<i>Best regards,</i>
`

const photoUrl = "/home/hanh/images/bot_support_teacher.jpeg";

export async function hello(bot, isHello = true){
    globalBotInstance = bot;
    // for(let username in listUser){
        // let chatId = listUsername[username]
        // bot.sendMessage(chatId, "Hello, I am Bitu. Have a good day!");
    // }
   if(isHello){
       bot.sendMessage(config.external.chat_id_telegram_health_check, "Hello you, I am Bitu. Have a good day!");
   }
    // get register user
    let initUserProfile = await models.UserProfile.findAll({
    limit: 2000,
    raw: true
    })
    let initUserIds = initUserProfile.map(i => i.userId)
    let listUserFromIds = await models.User.findAll({
    where: {id: initUserIds},
    raw: true
    })
    let listUserFromDbObj = {}
    for(let item of listUserFromIds){
        listUserFromDbObj[item.id] = item
        }
    let listUserTag = await models.UserTag.findAll({
    raw: true
    })
    let listUserTagObj = {}
    for(let item of listUserTag){
        if(!listUserTagObj[item.userId]) listUserTagObj[item.userId] = {}
        listUserTagObj[item.userId][item.tag] = 1
    }
    dataInit.user = {}
    dataInit.userByTeleCode = {}
    dataInit.userByUserName = {}
    dataInit.threadTutor = {}
    dataInit.threadTools = {}
    dataInit.chatTag = {}
    for(let item of initUserProfile){
        item.fullName = item.userName
        if(listUserFromDbObj[item.userId]){
            item.fullName = listUserFromDbObj[item.userId].lastName + " " + listUserFromDbObj[item.userId].firstName
        }
        setQueueChatPrivate(item.teleChatId, "vh-support-teacher")
        dataInit.user[item.teleChatId] = item
        dataInit.userByTeleCode[item.teleNameCode] = item
        dataInit.userByUserName[item.userName] = item
        dataInit.chatTag[item.teleChatId] = {}
        if(listUserTagObj[item.userId]){
            dataInit.chatTag[item.teleChatId] = listUserTagObj[item.userId]
            /*if(dataInit.chatTag[item.teleChatId] == 'sale_tutor'){
                setQueueChatPrivate(targetProfile.teleChatId, "chatvd-sale-tutor")
            }
            else if(dataInit.chatTag[item.teleChatId] == 'sale_tools'){
                        setQueueChatPrivate(targetProfile.teleChatId, "chatvd-sale-tools")
                    }*/
            setQueueChatPrivate(item.teleChatId, "vh-support-teacher")
        }
    }
    if(isHello){
        bot.sendMessage(config.external.chat_id_telegram_health_check, "Load register user DONE. Vd: " +  (dataInit.user[151333233] ? JSON.stringify(dataInit.user[151333233]) : 'no data'))
    }
 //    let threadTutor = await fileLogDataVdSupportTopicTutor.findAll()
 //    for(let item of threadTutor){
    // dataInit.threadTutor[item.teleNameCode] = item.threaId
 //    }
 //    if(isHello){
 //      bot.sendMessage(config.external.chat_id_telegram_health_check, "Load threadTutor DONE. Vd: " + (dataInit.threadTutor['TE_HaNH'] ? JSON.stringify(dataInit.threadTutor['TE_HaNH']) : "no data"))
 //    }
 //    let threadTools = await fileLogDataVdSupportTopicTools.findAll()
 //    for(let item of threadTools){
    // dataInit.threadTools[item.teleNameCode] = item.threaId
 //    }
 //    if(isHello){
 //         bot.sendMessage(config.external.chat_id_telegram_health_check, "Load threadTools DONE. Vd: " + (dataInit.threadTools['TE_HaNH'] ? JSON.stringify(dataInit.threadTools['TE_HaNH']) : "no data"))
 //    }
 //    dataInit.threadSupportZalo = {}
 //    let threadSupportZalo = await fileLogDataVdSupportTopicZalo.findAll()
 //    for(let item of threadSupportZalo){
 //        dataInit.threadSupportZalo[item.teleNameCode] = item.threaId
 //    }
 //    if(isHello){
 //         bot.sendMessage(config.external.chat_id_telegram_health_check, "Load threadSupportZalo DONE. Vd: " + (dataInit.threadSupportZalo['TE_HaNH'] ? JSON.stringify(dataInit.threadSupportZalo['TE_HaNH']) : "no data"))
 //    }
    dataInit.teacherSupportAnswer = {}
    let listTeacherSupportAnswer = await fileLogDataTeacherSupportAnswer.findAll()
    for(let item of listTeacherSupportAnswer){
    dataInit.teacherSupportAnswer[item.code] = item.content
    }
    if(isHello){
        bot.sendMessage(config.external.chat_id_telegram_health_check, "Load teacher support answer DONE. Vd: " + (dataInit.teacherSupportAnswer['1.'] ? dataInit.teacherSupportAnswer['1.'] : 'no data'))
    }
    dataInit.threadCrmError = {}
    dataInit.threadName = {}
    dataInit.threadIdName = {}
    let threadCrmError = await fileLogDataItSupportTopicSale.findAll()
    console.log('threadCrmError', threadCrmError)
    for(let item of threadCrmError){
        if(dataInit.threadIdName[item.threaId]){
            delete dataInit.threadName[dataInit.threadIdName[item.threaId]]
        }
       dataInit.threadCrmError[item.teleNameCode] = item.threaId
       dataInit.threadName[item.threadName] = item
       if(item.threadName){
            dataInit.threadIdName[item.threaId] = item.threadName
       }

    }
    dataInit.threadNameWait = {}
    for(let item of fileLogDataSetThreadWait){
        dataInit.threadNameWait[item.threaId] = item
    }
    if(isHello){
        bot.sendMessage(config.external.chat_id_telegram_health_check, "Load threadCrmError DONE. Vd: " + (dataInit.threadCrmError['TE_HaNH'] ? JSON.stringify(dataInit.threadCrmError['TE_HaNH']) : "no data"))
    }
    setTimeout(async function(){
        console.log('======== Reload Data =========')
        await hello(bot, false)
    }, 60000)
    //'-611501439'
    /*bot.sendMessage('-611501439', "Hello, I am Bitu. Have a good day!  @DVKH_ThanhNT3 @DVKH_AnhVL"); // van don team
    bot.sendMessage('-1001939275437', "Hello, I am Bitu. Have a good day!"); //van don Duo
    bot.sendMessage('-1001909508045', "Hello, I am Bitu. Have a good day!"); // van don THPT
    bot.sendMessage('-1001852905869', "Hello, I am Bitu. Have a good day!"); // van don Gia su
*/
//bot.sendMessage('-1001939275437', "Hello, I am Bitu. Have a good day!");
}

const HActiveRecord = require('../../hac/model/HActiveRecord')
const HActiveRecordFile = require('../../hac/model/HActiveRecordFile')
const fileLogData = new HActiveRecordFile({
    fileName: 'list_sale',
    recordAttribute: ['id', 'tId', 'username'],
    expired: 86400 * 30 * 12 * 5,
    limit: 10000
})
// const fileLogDataSaleVd = new HActiveRecordFile({
//                         fileName: 'sale_vd_1',
//                         recordAttribute: ['coreUserId', 'idByName', 'msg'],
//                         expired: 86400 * 30 * 12,
//                         limit: 1000
// })

// const fileLogDataSaleRequest = new HActiveRecordFile({
//      fileName: 'list_sale_request',
//      recordAttribute: ['id', 'tId', 'username', 'tUsername'],
//      expired: 86400 * 30 * 12 * 5,
//      limit: 10000
// })

const fileLogDataChatBot = new HActiveRecordFile({
            fileName: 'chat_bot_msg',
            recordAttribute: ['fromId', 'msgId', 'botMsgId'],
            expired: 86400 * 30 * 12 * 5,
            limit: 10000
})
// const fileLogDataVdSupportTopicTutor = new HActiveRecordFile({
//             fileName: 'chat_vd_support_topic_tutor',
//             recordAttribute: ['teleNameCode', 'chatId', 'threaId'],
//             expired: 86400 * 30 * 12 * 5,
//             limit: 10000
// })
// const fileLogDataVdSupportTopicZalo = new HActiveRecordFile({
//             fileName: 'chat_vd_support_topic_zalo',
//             recordAttribute: ['teleNameCode', 'chatId', 'threaId'],
//             expired: 86400 * 30 * 12 * 5,
//             limit: 10000
// })
// const fileLogDataVdSupportTopicTools = new HActiveRecordFile({
//             fileName: 'chat_vd_support_topic_tools',
//             recordAttribute: ['teleNameCode', 'chatId', 'threaId'],
//             expired: 86400 * 30 * 12 * 5,
//             limit: 10000
// })
const fileLogDataItSupportTopicSale = new HActiveRecordFile({
    fileName: 'chat_id_support_vh_for_teacher_topic',
    recordAttribute: ['teleNameCode', 'chatId', 'threaId', 'threadName', 'teleUserName'],
        expired: 86400 * 30 * 12 * 5,
        limit: 10000
})
const fileLogDataTeacherSupportAnswer = new HActiveRecordFile({
    fileName: 'chat_teacher_support_answer',
    recordAttribute: ['code', 'content'],
            expired: 86400 * 30 * 12 * 5,
            limit: 10000
})
const fileLogDataSetThreadWait = new HActiveRecordFile({
    fileName: 'chat_id_support_vh_for_teacher_thread_wait',
    recordAttribute: ['teleNameCode', 'chatId', 'threaId', 'threadName', 'teleUserName'],
        expired: 86400 * 30 * 12 * 5,
        limit: 10000
})
// Lịch sử broadcast theo nhóm
// recordAttribute: timestamp || senderId || groupCode || messageHtml || mediaType || totalSent || totalFailed
const fileLogBroadcastHistory = new HActiveRecordFile({
    fileName: 'broadcast_history',
    recordAttribute: ['timestamp', 'senderId', 'groupCode', 'messageHtml', 'mediaType', 'totalSent', 'totalFailed'],
    expired: 86400 * 30 * 12 * 2,
    limit: 50000
})
// const fileLogDataTopic = new HActiveRecordFile({
//    fileName: 'chat_CSHV_support_topic',
//    recordAttribute: ['teleNameCode', 'chatId', 'threaId'],
//    expired: 86400 * 30 * 12 * 5,
//    limit: 10000
// })
// const fileDataMsgReplyCshv = new HActiveRecordFile({
//              fileName: 'chat_bot_cshv_msg',
//              recordAttribute: ['chatId', 'orgMsgId', 'botMsgId', 'targetId'],
//              expired: 86400 * 30 * 12 * 5,
//              limit: 10000
// })
let queueChatPrivatePrev = {}
let queueChatPrivate = {}
let queueChatPrivateTime = {}
let queueChatPrivateMsg = {}
let queueChatPrivateMsgWait = {}
export function resetQueueChatPrivate(bot){
    for(let cId in queueChatPrivate){
            //if(cId != "151333233") continue
            if(queueChatPrivateTime[cId] < (Date.now() - 15 * 60 * 1000)){
                    //bot.sendMessage(cId, "Phiên trò chuyện của bạn đã hết hạn. Nếu bạn có yêu cầu hỗ trợ gì khác, vui lòng gõ /start")
                    //delete queueChatPrivate[cId]
                    //delete queueChatPrivateTime[cId]
            }
    }
    setTimeout(function(){
        resetQueueChatPrivate(bot)
    }, 1 * 60 * 1000)
}
//resetQueueChatPrivate()
async function executeBroadcast(msg, photoFileIds) {
    // photoFileIds: null (text/doc/video) | [fid] (1 ảnh) | [fid1,fid2,...] (album)
    const bot = globalBotInstance;
    const senderId = msg?.from?.id;
    const senderChatId = msg?.chat?.id;
    const commandText = (msg.caption ? msg.caption : (msg.text ? msg.text : '')).trim();
    const broadcastCommand = parseBroadcastCommand(commandText);
    if (!broadcastCommand) return;

    const groupCode = normalizeGroupCode(broadcastCommand.groupCode);
    const groupMembers = await models.UserGroup.findAll({ where: { groupCode }, raw: true });
    const teacherCodes = (groupMembers || []).map(m => normalizeTeacherCode(m.userName)).filter(Boolean);
    if (!teacherCodes.length) {
        await bot.sendMessage(senderChatId, `Nhóm <b>${escapeHtml(groupCode)}</b> không có thành viên nào trong bảng user_group.`, { parse_mode: 'HTML' });
        return;
    }

    // Convert nội dung lệnh sang HTML
    const entities = msg.caption ? (msg.caption_entities || []) : (msg.entities || []);
    const { messageStart: start, messageEnd: end, messageText: rawText } = broadcastCommand;
    let messageHtml = '';
    if (typeof start === 'number' && typeof end === 'number') {
        messageHtml = renderTelegramEntitiesToHtml(commandText.slice(start, end), sliceEntitiesForSubstring(entities, start, end));
    } else {
        messageHtml = renderTelegramEntitiesToHtml(rawText, []);
    }
    const sendOption = { parse_mode: 'HTML' };

    // Xác định loại media
    const isAlbum = photoFileIds && photoFileIds.length > 1;
    const isSinglePhoto = photoFileIds && photoFileIds.length === 1;

    const result = { success: [], failed: [], total: teacherCodes.length };
    for (let i = 0; i < teacherCodes.length; i++) {
        const teacherCode = teacherCodes[i];
        const userProfile = dataInit.userByUserName[teacherCode];
        if (!userProfile || !userProfile.teleChatId) {
            result.failed.push({ teacherCode, reason: 'Không tìm thấy giáo viên hoặc giáo viên chưa đăng ký Telegram' });
            continue;
        }
        try {
            if (isAlbum) {
                const mediaArray = photoFileIds.map((fid, idx) => ({
                    type: 'photo',
                    media: fid,
                    ...(idx === 0 && messageHtml ? { caption: messageHtml, parse_mode: 'HTML' } : {}),
                }));
                await bot.sendMediaGroup(userProfile.teleChatId, mediaArray);
            } else if (isSinglePhoto) {
                await bot.sendPhoto(userProfile.teleChatId, photoFileIds[0], { ...sendOption, caption: messageHtml });
            } else if (msg.document) {
                await bot.sendDocument(userProfile.teleChatId, msg.document.file_id, { ...sendOption, caption: messageHtml });
            } else if (msg.video) {
                await bot.sendVideo(userProfile.teleChatId, msg.video.file_id, { ...sendOption, caption: messageHtml });
            } else {
                await bot.sendMessage(userProfile.teleChatId, messageHtml, sendOption);
            }
            result.success.push({ teacherCode, teleChatId: userProfile.teleChatId, userName: userProfile.userName });
        } catch (error) {
            result.failed.push({ teacherCode, teleChatId: userProfile.teleChatId, userName: userProfile.userName, reason: error.message || 'Lỗi không xác định' });
        }
        if (i < teacherCodes.length - 1) await new Promise(r => setTimeout(r, 300));
    }

    // Ghi lịch sử broadcast
    try {
        const mediaType = isAlbum ? `album_${photoFileIds.length}` : isSinglePhoto ? 'photo' : msg.document ? 'document' : msg.video ? 'video' : 'text';
        const ts = new Date().toISOString();
        await fileLogBroadcastHistory.set(
            `${ts} || ${senderId} || ${groupCode} || ${messageHtml.replace(/\|\|/g, '|')} || ${mediaType} || ${result.success.length} || ${result.failed.length}`
        );
    } catch (historyError) {
        console.error('Lỗi ghi lịch sử broadcast:', historyError);
    }

    // Ack lại cho admin
    const ackLines = [
        `<b>Broadcast</b> nhóm <b>${escapeHtml(groupCode)}</b>`,
        `- Tổng: <b>${result.total}</b>`,
        `- Thành công: <b>${result.success.length}</b>`,
        `- Thất bại: <b>${result.failed.length}</b>`
    ];
    if (result.failed.length) {
        const failedPreview = result.failed.slice(0, 20).map(f => `- <code>${escapeHtml(f.teacherCode || '')}</code>: ${escapeHtml(f.reason || '')}`);
        ackLines.push('', `<b>Danh sách lỗi (tối đa 20)</b>`, ...failedPreview);
    }
    await bot.sendMessage(senderChatId, ackLines.join('\n'), { parse_mode: 'HTML' });
}

export async function receiveMsg(bot){
    try{
    bot.on("callback_query", async (callbackQuery) => {
        const data = callbackQuery.data;
        console.log('callback_query', data)
        queueChatPrivate[callbackQuery.message.chat.id] = data
        queueChatPrivateTime[callbackQuery.message.chat.id] = Date.now()
        if (data === "dktele") {
            // bot.sendMessage(callbackQuery.message.chat.id, "Anh/chị vui lòng truy cập vào trang https://crm.rinoedu.ai/profile hoặc https://erp.rinoedu.ai/user để copy 6 chữ số Mã đăng ký Telegram và gửi vàođây cho em ạ")
              bot.sendPhoto(callbackQuery.message.chat.id, photoUrl, { caption, parse_mode: "HTML" })
        } else if (data === "none") {
            bot.sendMessage(callbackQuery.message.chat.id, "Chúc anh/chị 1 ngày làm việc hiệu quả!")
            delete queueChatPrivate[callbackQuery.message.chat.id]
        } else if (data === "chat") {
            bot.sendMessage(callbackQuery.message.chat.id, "Nếu cần bất kỳ hỗ trợ gì, anh/chị vui lòng gõ /start ạ")
        }
        else if(data == "notification"){
            bot.sendMessage(callbackQuery.message.chat.id, "Please select one", {
                reply_markup: JSON.stringify({
                                         inline_keyboard: [
                                            [{ text: "Notification username", callback_data: "notif_username" }],
                                            //[{ text: "Notification register", callback_data: "notif_register" }],
                     ]
                                })
            })
        }
        else if(data == "join-crm"){
            try{
                await bot.unbanChatMember('-1001823735334', callbackQuery.message.chat.id)
                let iL = await bot.createChatInviteLink('-1001823735334',{member_limit:1})
                iL = iL.invite_link
                await bot.sendMessage(callbackQuery.message.chat.id, "Bạn tham gia bằng link này nhé: " + iL)
                                await bot.sendMessage("151333233", "send link " + callbackQuery.message.chat.id + " " + iL)
                delete queueChatPrivate[callbackQuery.message.chat.id]
            }
            catch(eIL){
                console.log(eIL)
            }
        }
        else if(data == "notif_register"){

        }
        else if(data == "notif_username"){
            for(let cId in dataInit.user){
                if(!dataInit.user[cId].teleOtp){
                    try{
                        await bot.sendMessage(dataInit.user[cId].teleChatId, "Xin chào anh/chị, em là Bitu. Anh/chị xác nhận mình có phải mã nhân viên " + dataInit.user[cId].userName + " không ạ?")
                        setQueueChatPrivate(dataInit.user[cId].teleChatId, "chat")
                        bot.sendMessage(151333233, "Send confirm username to " + dataInit.user[cId].teleNameCode)
                        await models.UserProfile.update({
                                                        teleOtp: '-11111'
                                                        }, {
                                                        where: {
                                                                teleChatId: cId
                                                        }
                                                })
                                                dataInit.user[cId].teleOtp = '-11111'
                        return
                    }
                    catch(e){
                        bot.sendMessage(151333233, "Need register " + dataInit.user[cId].teleNameCode)
                        await models.UserProfile.update({
                                                    teleOtp: '000000'
                                                }, {
                                                    where: {
                                                            teleChatId: cId
                                                    }
                                            })
                                            dataInit.user[cId].teleOtp = '000000'
                    }
                    break
                }
            }
        }
        else if(data == 'chatvd-sale-tutor' || data == 'chat-teacher-vi' || data == 'chatvd-sale-tools'){
            if(queueChatPrivateMsgWait[callbackQuery.message.chat.id] && 0){

            }
            else{
                bot.sendMessage(callbackQuery.message.chat.id, "Xin chào, bạn cần hỗ trợ gì ạ?")
            }
        }
        else if(data == 'vh-support-teacher'){
                bot.sendMessage(callbackQuery.message.chat.id, "Xin chào, bạn cần hỗ trợ gì ạ?")
        }
        //bot.sendMessage(callbackQuery.message.chat.id, "You clicked button " + data);
    });

    bot.on("message", async (msg) => {
        console.log('msg', msg, queueChatPrivate[msg.chat.id], dataInit.user[msg.chat.id])
        //bot.sendMessage(config.external.chat_id_telegram_health_check, JSON.stringify(msg))
        let rContent = JSON.stringify(msg)
        let rPhoto = null
        let rMediaGroupId = null
        if(msg.photo){
            rPhoto = await downloadFileFromMessageTele(bot, msg)
        }
        if(msg.media_group_id){
            rMediaGroupId = msg.media_group_id
        }
        let rVideo = null;
        if (msg.video) {
            const videoSize = msg.video.file_size || 0;
            // Nếu video > 50MB thì KHÔNG download, dùng copyMessage
            if (videoSize > 49 * 1024 * 1024 || 1) {
                rVideo = 'forwarded'
                // let forwarded = await bot.copyMessage(
                //     groupSupportTarget,
                //     msg.chat.id,
                //     msg.message_id,
                //     {
                //         caption: msg.caption || rContent,
                //         parse_mode: "HTML",
                //         message_thread_id: rOption.message_thread_id
                //     }
                // );
                // await fileLogDataChatBot.set(
                //     `${msg.chat.id} || ${msg.message_id} || ${forwarded.message_id}`
                // );
                // return;
            }
            else{
                // Video nhỏ → vẫn dùng download + sendVideo
                const videoFile = await downloadFileFromMessageTele(bot, msg);
                rVideo = {
                    dataBuffer: videoFile.dataBuffer,
                    fileName: videoFile.fileName || "video.mp4",
                    mimeType: msg.video.mime_type || "video/mp4"
                };
            }
        }
        let groupSupport = '151333233'
        let groupSupportVh = '-4130384459'
        let groupSupportTarget = '151333233'
        let rOption = { parse_mode: 'HTML' }
        let teleNameForUser = msg.from.id
        let teleNameCode = ''
        if(msg.caption) rContent = msg.caption
        else if(msg.text) rContent = msg.text
        else rContent = ''

        // ===== Broadcast command: "Gửi tin nhắn 'XXX' cho nhóm AAA" =====
        try {
            const senderId = msg?.from?.id;
            const isAllowedSender = allowedBroadcastSenderIds.has(senderId) || allowedBroadcastSenderIds.has(String(senderId));

            if (isAllowedSender && msg.media_group_id) {
                // Album ảnh: gom tất cả msg cùng media_group_id qua throttleBroadcastAlbum
                // throttleByKey sẽ đợi 1.5s sau msg cuối cùng rồi mới gọi executeBroadcast
                // → KHÔNG bị ảnh hưởng bởi await downloadFileFromMessageTele ở trên
                pendingBroadcastAlbums[msg.media_group_id] = true; // đánh dấu để throttle nhận
                throttleBroadcastAlbum(msg.media_group_id, msg);
                return;
            }

            const commandText = (msg.caption ? msg.caption : (msg.text ? msg.text : '')).trim();
            const broadcastCommand = parseBroadcastCommand(commandText);
            if (broadcastCommand && isAllowedSender) {
                // Ảnh đơn hoặc text: xử lý trực tiếp, không cần gom album
                const photos = msg.photo ? [[...msg.photo].pop().file_id] : null;
                await executeBroadcast(msg, photos);
                return;
            }
        } catch (broadcastError) {
            console.log('broadcastError', broadcastError);
        }
        // ===== End broadcast command =====

        if(msg.chat.id == groupSupportVh && rContent){
                    rContent = await reRenderContent(rContent)
                }
        if(msg.chat && (msg.chat.id == '-1003207607839_1' || msg.chat.id == '-1003207607839')
            && (msg.from.id == '151333233')){
            let textFormatEditName = '@rino_teacher_support_bot Cập nhật topic '
            if(msg.text && msg.text.indexOf(textFormatEditName) == 0){
                let msgTextArr = msg.text.split(textFormatEditName)
                if(msgTextArr.length == 2){
                    let neoTopicName = ': tên mới '
                    let neoTopicSetUsername = ': là của username '
                    let neoTopicSetChatId = ': là của id '
                    let neoTopicSetNameCode = ': là của GV '
                    if(msgTextArr[1].indexOf(neoTopicSetNameCode) >= 0){
                        let dataEditArr = msgTextArr[1].split(neoTopicSetNameCode)
                        if(dataEditArr.length == 2){
                            let topicNameOld = dataEditArr[0].trim()
                            let topicNameCodeNew = dataEditArr[1].trim()
                            let topicEditId = 0
                            let topicTeleUserName = ''
                            let topicUserChatId = ''
                            for(let _topicName in dataInit.threadName){//_topicName is chatId
                                if(_topicName == topicNameOld){
                                    topicEditId = dataInit.threadName[_topicName].threaId
                                    if(dataInit.threadName[_topicName].teleUserName){
                                        topicTeleUserName = dataInit.threadName[_topicName].teleUserName
                                    }
                                    topicUserChatId = dataInit.threadName[_topicName].teleNameCode
                                }
                            }
                            if(!topicEditId){
                                bot.sendMessage(msg.chat.id, `Topic ${topicNameOld} không tồn tại`, {
                                    parse_mode: 'HTML',
                                    reply_to_message_id: msg.message_id
                                })
                            }
                            else{
                                 if(topicUserChatId){
                                    await models.UserProfile.update({
                                        userName: topicNameCodeNew
                                    }, {
                                        where: {
                                            teleChatId: topicUserChatId
                                        }
                                    })
                                    dataInit.user[topicUserChatId].userName = topicNameCodeNew
                                 }
                            }
                        }
                    }
                    else if(msgTextArr[1].indexOf(neoTopicSetUsername) >= 0){
                        let dataEditArr = msgTextArr[1].split(neoTopicSetUsername)
                        if(dataEditArr.length == 2){
                            let topicNameOld = dataEditArr[0].trim()
                            let topicUserNameNew = dataEditArr[1].trim()
                            let topicEditId = 0
                            for(let _topicName in dataInit.threadName){//_topicName is chatId
                                if(_topicName == topicNameOld){
                                    topicEditId = dataInit.threadName[_topicName].threaId
                                }
                            }
                            if(!topicEditId){
                                bot.sendMessage(msg.chat.id, `Topic ${topicNameOld} không tồn tại`, {
                                    parse_mode: 'HTML',
                                    reply_to_message_id: msg.message_id
                                })
                            }
                            else{
                                if(topicUserNameNew && !dataInit.userByTeleCode[topicUserNameNew]){
                                    await fileLogDataSetThreadWait.set(`0 || -1003207607839_1 || ${topicEditId} || ${topicNameOld} || ${topicUserNameNew}`)
                                    dataInit.threadNameWait[topicEditId] = {
                                        'teleNameCode': 0,
                                        'chatId': '-1003207607839_1',
                                        'threaId': topicEditId,
                                        'threadName': topicNameOld,
                                        teleUserName: topicUserNameNew
                                    }
                                }
                                else if(topicUserNameNew && dataInit.userByTeleCode[topicUserNameNew]){
                                    await fileLogDataItSupportTopicSale.set(`${dataInit.userByTeleCode[topicUserNameNew].teleChatId} || -1003207607839_1 || ${topicEditId} || ${topicNameOld} || ${topicUserNameNew}`)
                                    dataInit.threadName[topicNameOld] = {
                                        'teleNameCode': dataInit.userByTeleCode[topicUserNameNew].teleChatId,
                                        'chatId': '-1003207607839_1',
                                        'threaId': topicEditId,
                                        'threadName': topicNameOld
                                    }
                                }
                                // dataInit.user[msg.chat.id].userName
                                bot.sendMessage(msg.chat.id, `Đã setup xong. Bạn báo user đó nhắn tin với mình là tin nhắn sẽ vào topic ${topicNameOld}`, {
                                    parse_mode: 'HTML',
                                    reply_to_message_id: msg.message_id
                                })
                            }
                        }
                    }
                    else if(msgTextArr[1].indexOf(neoTopicName) >= 0){
                        let dataEditArr = msgTextArr[1].split(neoTopicName)
                        if(dataEditArr.length == 2){
                            let topicNameOld = dataEditArr[0].trim()
                            let topicNameNew = dataEditArr[1].trim()
                            if(topicNameOld != topicNameNew){
                                let topicEditId = 0
                                let topicUserChatId = 0
                                let topicUserNameNew = ''
                                for(let _topicName in dataInit.threadName){//_topicName is chatId
                                    if(_topicName == topicNameOld){
                                        topicEditId = dataInit.threadName[_topicName].threaId
                                        topicUserChatId = dataInit.threadName[_topicName].teleNameCode
                                        topicUserNameNew = dataInit.threadName[_topicName].teleUserName
                                        dataInit.threadName[topicNameNew] = dataInit.threadName[_topicName]
                                    }
                                }
                                if(topicEditId && topicUserChatId){
                                    bot.editForumTopic('-1003207607839', topicEditId, topicNameNew)
                                    .then(async result => {
                                        if (result) {
                                            console.log(`Đã đổi tên topic ${topicEditId} thành công.`);
                                            await fileLogDataItSupportTopicSale.set(`${topicUserChatId} || -1003207607839_1 || ${topicEditId} || ${topicNameNew} || `)
                                            delete dataInit.threadName[topicNameOld]
                                            dataInit.threadName[topicNameNew] = {
                                                'teleNameCode': topicUserChatId,
                                                'chatId': '-1003207607839_1',
                                                'threaId': topicEditId,
                                                'threadName': topicNameNew
                                            }
                                        }
                                    })
                                    .catch(error1 => {
                                        console.error("Lỗi khi sửa tên topic:", error1);
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
        if(msg.from.username){
            teleNameForUser = msg.from.username
        }
        teleNameCode = dataInit.user[msg.chat.id] && dataInit.user[msg.chat.id].teleNameCode
            ? dataInit.user[msg.chat.id].teleNameCode : teleNameForUser
        if(msg.chat && (msg.chat.id == '-1003207607839_1' || msg.chat.id == '-1003207607839') && msg.message_thread_id){
               msg.chat.id = '-1003207607839_1'
            for(let _topicName in dataInit.threadCrmError){//_topicName is chatId
            // console.log('_topicName', _topicName, msg.message_thread_id, dataInit.userByTeleCode[_topicName])
                if(dataInit.threadCrmError[_topicName] == msg.message_thread_id && dataInit.user[_topicName]){
                        groupSupportTarget = _topicName//dataInit.userByTeleCode[_topicName].teleChatId
                        setQueueChatPrivate(groupSupportTarget, "vh-support-teacher")
                }
            }
            if(msg.text && (msg.from.id == '151333233' || msg.from.id == '6696592763' || msg.from.id == '5083898895')){

                if(msg.text.indexOf('@rino_teacher_support_bot đổi tên topic:') >= 0){
                    groupSupportTarget = '151333233'
                    let msgTextArr = msg.text.split('@rino_teacher_support_bot đổi tên topic:')
                    if(msgTextArr[1]){
                        let topicUserChatId = ''
                        let topicNameOld = ''
                        let topicNameNew = msgTextArr[1].trim()
                        for(let _topicName in dataInit.threadName){//_topicName is chatId
                            if(dataInit.threadName[_topicName].threaId == msg.message_thread_id){
                                topicNameOld = _topicName
                                topicUserChatId = dataInit.threadName[_topicName].teleNameCode
                            }
                        }
                        if(topicNameOld && topicNameNew != topicNameOld && topicUserChatId){
                            bot.editForumTopic('-1003207607839', msg.message_thread_id, topicNameNew)
                            .then(async result => {
                                if (result) {
                                    console.log(`Đã đổi tên topic ${msg.message_thread_id} thành công.`);
                                    await fileLogDataItSupportTopicSale.set(`${topicUserChatId} || -1003207607839_1 || ${msg.message_thread_id} || ${topicNameNew} || `)
                                    delete dataInit.threadName[topicNameOld]
                                    dataInit.threadName[topicNameNew] = {
                                        'teleNameCode': topicUserChatId,
                                        'chatId': '-1003207607839_1',
                                        'threaId': msg.message_thread_id,
                                        'threadName': topicNameNew
                                    }
                                    bot.sendMessage(msg.chat.id, `Đã setup xong.`, {
                                        parse_mode: 'HTML',
                                        reply_to_message_id: msg.message_id
                                    })
                                }
                            })
                            .catch(error1 => {
                                console.error("Lỗi khi sửa tên topic:", error1);
                            });
                        }
                    }
                }
                else if(msg.text.indexOf('@rino_teacher_support_bot đổi GV:') >= 0){
                    groupSupportTarget = '151333233'
                    let msgTextArr = msg.text.split('@rino_teacher_support_bot đổi GV:')
                    if(msgTextArr[1]){
                        let topicNameCodeNew = msgTextArr[1].trim()
                        let topicUserChatId = ''
                        for(let _topicName in dataInit.threadName){//_topicName is chatId
                            if(dataInit.threadName[_topicName].threaId == msg.message_thread_id){
                                topicUserChatId = dataInit.threadName[_topicName].teleNameCode
                            }
                        }
                        if(topicUserChatId){
                            await models.UserProfile.update({
                                userName: topicNameCodeNew
                            }, {
                                where: {
                                    teleChatId: topicUserChatId
                                }
                            })
                            dataInit.user[topicUserChatId].userName = topicNameCodeNew
                            bot.sendMessage(msg.chat.id, `Đã setup xong.`, {
                                parse_mode: 'HTML',
                                reply_to_message_id: msg.message_id
                            })
                         }
                    }
                }
            }
        }
        else if(msg.chat && dataInit.user[msg.chat.id] && queueChatPrivate[msg.chat.id] == 'vh-support-teacher'){


            let topicNameForUser = ''
            if(dataInit.user[msg.chat.id] && dataInit.user[msg.chat.id].fullName){
                topicNameForUser = dataInit.user[msg.chat.id].fullName + '('
            }
            if(dataInit.user[msg.chat.id] && dataInit.user[msg.chat.id].userName){
                topicNameForUser = topicNameForUser + dataInit.user[msg.chat.id].userName
            }
            else{
                topicNameForUser = topicNameForUser + teleNameCode
            }
            if(dataInit.user[msg.chat.id] && dataInit.user[msg.chat.id].fullName){
                topicNameForUser = topicNameForUser + ')'
            }
            let isWaitSetup = 0
            let isWaitSetupTopicName = ''
            for(let threadWait in dataInit.threadNameWait){
                if(msg.from.username
                    && dataInit.threadNameWait[threadWait].teleUserName == msg.from.username
                    && dataInit.threadNameWait[threadWait].teleNameCode == 0){
                    isWaitSetup = dataInit.threadNameWait[threadWait].threaId
                    isWaitSetupTopicName = dataInit.threadNameWait[threadWait].threadName
                    dataInit.threadNameWait[threadWait].teleNameCode = msg.chat.id
                }
            }
            if(isWaitSetup){
                await fileLogDataItSupportTopicSale.set(`${msg.chat.id} || -1003207607839_1 || ${isWaitSetup} || ${topicNameForUser}`)
                dataInit.threadCrmError[msg.chat.id] = isWaitSetup
                // dataInit.user[msg.chat.id].userName =
            }
            else if(!dataInit.threadCrmError[msg.chat.id]){
                let ctpFromSale = await bot.createForumTopic('-1003207607839_1', topicNameForUser)
                 await fileLogDataItSupportTopicSale.set(`${msg.chat.id} || -1003207607839_1 || ${ctpFromSale.message_thread_id} || ${topicNameForUser} || ${teleNameForUser}`)
                dataInit.threadCrmError[msg.chat.id] = ctpFromSale.message_thread_id
                dataInit.threadName[topicNameForUser] = {
                    'teleNameCode': msg.chat.id,
                    'chatId': '-1003207607839_1',
                    'threaId': ctpFromSale.message_thread_id,
                    'threadName': topicNameForUser
                }
            }
            groupSupportTarget = '-1003207607839_1'
            rOption.message_thread_id = dataInit.threadCrmError[msg.chat.id]
        }
        if(msg.reply_to_message){
            let dbs = await fileLogDataChatBot.findAll()
            //console.log('dbs', dbs)
            let dbsFound = false
            for(let dbsItem of dbs){
                            if(dbsItem.botMsgId == msg.reply_to_message.message_id && dbsItem.fromId == groupSupportTarget){
                                    rOption.reply_to_message_id = dbsItem.msgId
                    dbsFound = true
                                }
                        }
            if(!dbsFound){
                rContent = ''
                if(msg.reply_to_message.text){
                    rContent += msg.reply_to_message.text
                }
                else if(msg.reply_to_message.caption){
                    rContent += msg.reply_to_message.caption
                }
                if(rContent && msg.caption){
                    rContent +=  '\n ====> ' + msg.caption
                }
                else if(rContent && msg.text){
                    rContent +=  '\n ====> ' + msg.text
                }
                else if(!rContent){
                    rContent = msg.caption ? msg.caption : (msg.text ? msg.text : '')
                }
            }
            else{
                let getOrderCodeFromOrg = getOrderCode(rContent)
                            if(getOrderCodeFromOrg.length){
                                    rContent = getOrderCodeFromOrg.join(', ') + rContent
                            }
            }
        }
        if(rPhoto && !rMediaGroupId){
            rOption.filename = rPhoto.fileName
            rOption.caption = rContent
            let bs = await bot.sendPhoto(groupSupportTarget, rPhoto.dataBuffer, rOption)
            await fileLogDataChatBot.set(`${msg.chat.id} || ${msg.message_id} || ${bs.message_id}`)
        }
        else if(rPhoto && rMediaGroupId){
            /*if(msg.reply_to_message && msg.reply_to_message.text){
                rContent = msg.reply_to_message.text + '\n ====> '
            }
            else{
                rContent = ''
            }
                        if(msg.caption) rContent += msg.caption
                        else if(msg.text) rContent += msg.text
            */

            throttleImageForGroup(rMediaGroupId, {
                    photo: rPhoto,
                    chatId: groupSupportTarget,//config.external.chat_id_telegram_health_check,
                    caption: rContent,
                bot: bot,
                option: rOption,
                msg: {
                    message_id: msg.message_id,
                    chat_id: msg.chat.id
                }
                });
        }
        else if (rVideo) {
            if(rVideo == 'forwarded'){
                let forwarded = await bot.copyMessage(
                    groupSupportTarget,
                    msg.chat.id,
                    msg.message_id,
                    {
                        caption: msg.caption || rContent,
                        parse_mode: "HTML",
                        message_thread_id: rOption.message_thread_id
                    }
                );
                await fileLogDataChatBot.set(
                    `${msg.chat.id} || ${msg.message_id} || ${forwarded.message_id}`
                );
            }
            else{
                rOption.caption = rContent;
                rOption.filename = rVideo.fileName;
                let bs = await bot.sendVideo(
                    groupSupportTarget,
                    {
                        filename: rVideo.fileName,
                        contentType: rVideo.mimeType,
                        stream: Buffer.from(rVideo.dataBuffer)
                    },
                    rOption
                );

                await fileLogDataChatBot.set(
                    `${msg.chat.id} || ${msg.message_id} || ${bs.message_id}`
                );
            }
        }
        else{
            if(groupSupportTarget == config.external.chat_id_telegram_health_check && !rContent){
                rContent = JSON.stringify(msg)
            }
            if(msg.chat.type == 'private'){
                if(msg.chat.username && dataInit.user[msg.chat.id] && (!dataInit.user[msg.chat.id].teleNameCode || dataInit.user[msg.chat.id].teleNameCode != msg.chat.username)){
                    await models.UserProfile.update({
                        teleNameCode: msg.chat.username
                    }, {
                        where: {
                            teleChatId: msg.chat.id
                        }
                    })
                    dataInit.user[msg.chat.id].teleNameCode = msg.chat.username
                }
                if(msg.chat.username && dataInit.user[msg.chat.id]
                    && (!dataInit.user[msg.chat.id].teleOtp || dataInit.user[msg.chat.id].teleOtp == '-11111' || dataInit.user[msg.chat.id].teleOtp == '000000')){
                    await models.UserProfile.update({
                                                teleOtp: '111111'
                                        }, {
                                                where: {
                            teleChatId: msg.chat.id
                                                }
                                        })
                                        dataInit.user[msg.chat.id].teleOtp = '111111'
                }
                let isNameCodeValid = false
                if(!dataInit.user[msg.chat.id] && queueChatPrivate[msg.chat.id] != 'dktele'){
                    // rOption = {
                    //  reply_markup: JSON.stringify({
                    //      inline_keyboard: [
                    //          [{ text: "Đăng ký tài khoản telegram", callback_data: "dktele"}],
                    //          //[{ text: "Không cần hỗ trợ", callback_data: "none"}]
                    //      ]
                    //  })
                    // }
                    // bot.sendMessage(msg.chat.id, "Xin chào, anh/chị muốn em hỗ trợ gì ạ?", rOption)
                    queueChatPrivate[msg.chat.id] = 'dktele'
                    queueChatPrivateTime[msg.chat.id] = Date.now()
                    // bot.sendMessage(msg.chat.id, "Anh/chị vui lòng truy cập vào trang https://crm.rinoedu.ai/profile hoặc https://erp.rinoedu.ai/user để copy 6 chữ số Mã đăng ký Telegram và gửi vào đây cho em ạ")
                    bot.sendPhoto(msg.chat.id, photoUrl, { caption, parse_mode: "HTML" })
                    return
                }
                // else if(queueChatPrivate[msg.chat.id] != 'dktele'){
                //  setQueueChatPrivate(msg.chat.id, "vh-support-teacher")
                //  bot.sendMessage(msg.chat.id, "Xin chào, bạn muốn hỗ trợ gì ạ?")
                //  return
                // }
                if(queueChatPrivate[msg.chat.id] == "chat"){
                    bot.sendMessage(msg.chat.id, "Em đã nhận được thông tin. Cảm ơn anh/chị!")
                    bot.sendMessage("151333233", JSON.stringify(msg))
                    return
                }

                let vhText = msg.text ? msg.text.trim() : ''
                if(queueChatPrivate[msg.chat.id] == "dktele"){
                    // neu otp da su dung thi bao ko hop le
                    let isOtpUsed = false
                    for(let _chatId in dataInit.user){
                        if(dataInit.user[_chatId].teleOtp == vhText){
                            isOtpUsed = true
                        }
                    }
                    if(isOtpUsed){
                        rContent = "Otp không hợp l?~G. Vui lòng nhập lại!"
                        bot.sendMessage(msg.chat.id, rContent)
                                                bot.sendMessage(config.external.chat_id_telegram_health_check, ('@' + msg.from.username + ': ' + rContent))
                        delete queueChatPrivate[msg.chat.id]
                        return
                    }
                    let userProfileObj = dataInit.user[msg.chat.id]
                        /*await models.UserProfile.findOne({
                        where: {
                            teleChatId: msg.chat.id
                        },
                        raw: true
                    })*/
                    if(userProfileObj){
                        let userProfileExist = await models.User.findOne({
                            where: {
                                id: userProfileObj.userId
                            },
                            raw: true
                        })
                        rContent = "Anh/chị " + userProfileExist.username + " đã được đăng ký từ trước rồi ạ. Cảm ơn anh/chị!"
                        delete queueChatPrivate[msg.chat.id]
                        bot.sendMessage(msg.chat.id, rContent)
                        bot.sendMessage(config.external.chat_id_telegram_health_check, ('@' + msg.from.username + ' dktele ' + userProfileExist.username))
                        if(!dataInit.user[msg.chat.id].teleOtp){
                                                        await models.UserProfile.update({
                                                                teleOtp: vhText
                                                        }, {
                                                                where: {
                                                                        userId: userProfileObj.userId
                                                                }
                                                        })
                            dataInit.user[msg.chat.id].teleOtp = vhText
                                                }
                        return
                    }
                    let vhUserId = decodeToOTP(vhText)
                    if(vhUserId){
                        let userObj = await models.User.findOne({
                                                      where: {
                                                                id: vhUserId
                                                        },
                                                        raw: true
                                                })
                                                if(userObj){
                            await models.UserProfile.create({
                                userId: userObj.id,
                                userName: userObj.username,
                                teleNameCode: msg.from.username,
                                teleChatId: msg.from.id,
                                teleOtp: vhText
                            })
                            dataInit.user[msg.chat.id] = {
                                userId: userObj.id, userName: userObj.username, teleNameCode: msg.from.username, teleChatId: msg.from.id
                            }
                                                        rContent = caption2.replace(/Username/g, userObj.username)
                                                    rOption = {reply_to_message_id: msg.message_id, parse_mode: "HTML"}
                            //delete queueChatPrivate[msg.chat.id]
                            setQueueChatPrivate(msg.chat.id, "vh-support-teacher")
                            let ctpFromTeacher = await bot.createForumTopic('-1003207607839_1', userObj.username)
                            await fileLogDataItSupportTopicSale.set(`${msg.chat.id} || -1003207607839_1 || ${ctpFromTeacher.message_thread_id} || ${userObj.username} || ${msg.from.username}`)
                            dataInit.threadCrmError[msg.chat.id] = ctpFromTeacher.message_thread_id
                            dataInit.threadName[userObj.username] = {
                                'teleNameCode': msg.chat.id,
                                'chatId': '-1003207607839_1',
                                'threaId': ctpFromTeacher.message_thread_id,
                                'threadName': userObj.username
                            }
                        }
                        else{
                            rOption = {
                                                        reply_markup: JSON.stringify({
                                                                inline_keyboard: [
                                                                        [{ text: "?~P?~Cng ký t?| i khoản telegram", callback_data: "dktele"}],
                                                                        //[{ text: "Không cần h?~W trợ", callback_data: "none"}]
                                                                ]
                                                        })
                                                }
                            rContent = "Otp không hợp lệ. Vui lòng nhập lại!"
                        }
                        bot.sendMessage(msg.chat.id, rContent, rOption)
                        bot.sendMessage(config.external.chat_id_telegram_health_check, ('@' + msg.from.username + ': ' + rContent))
                                                return
                    }
                    rOption.reply_to_message_id = msg.message_id
                }
                //groupSupportTarget = msg.chat.id
                if(msg.chat.id != config.external.chat_id_telegram_health_check && msg.chat.id != '879428163' && msg.chat.id != '989760784' && msg.chat.id != '1949636527' && !queueChatPrivate[msg.chat.id]){
                                    groupSupportTarget = config.external.chat_id_telegram_health_check
                                    await bot.sendMessage(groupSupportTarget, JSON.stringify(msg))
                                    return
                            }
            }
            // tin nhan den tu group hoac tn rieng cua Admin-Ha
            if(msg.chat.id == config.external.chat_id_telegram_health_check){
                rContent = await reRenderContent(rContent)
            }
            if(groupSupportTarget == config.external.chat_id_telegram_health_check && queueChatPrivate[msg.chat.id] == 'chat-teacher-vi'){

            }
            else if(groupSupportTarget == config.external.chat_id_telegram_health_check){
                rContent = JSON.stringify(msg)
            }

            if(msg.chat.id == "-1003207607839_1" && !msg.message_thread_id && msg.from.id == '151333233'){
                if(msg.text && msg.text.indexOf('@allsale ') == 0){
                    //let _saItemKey = 0
                    /*for(let _saItem in dataInit.threadCrmError){
                        if(dataInit.userByTeleCode[_saItem] && _saItem.toLowerCase().indexOf('sa') == 0){
                            try{
                                await bot.sendMessage(dataInit.userByTeleCode[_saItem].teleChatId, msg.text.replace('@allsale ', ''))
                                console.log('Send all SA to', dataInit.userByTeleCode[_saItem])
                            } catch(eS){
                                console.log('send all', eS)
                            }
                        }
                    }*/
                    for(let _saItem in dataInit.userByTeleCode){
                        if(_saItem.toLowerCase().indexOf('sa') == 0){
                            try{
                                await bot.sendMessage(dataInit.userByTeleCode[_saItem].teleChatId, msg.text.replace('@allsale ', ''))
                                console.log('Send all SA to', dataInit.userByTeleCode[_saItem])
                            } catch(eS){
                                console.log('ERROR Send all', dataInit.userByTeleCode[_saItem])
                            }
                            await new Promise(r => setTimeout(() => r(), 300))
                        }
                        //_saItemKey++
                    }
                }
            }
            if(msg.chat.type == 'private'){
                if(dataInit.user[msg.chat.id] && dataInit.user[msg.chat.id].userName){
                    rContent = `(${dataInit.user[msg.chat.id].userName}) ` + rContent
                }
                else if(teleNameCode){
                    rContent = `(${teleNameCode}) ` + rContent
                }

            }
            let bs = await bot.sendMessage(groupSupportTarget, rContent, rOption)
            await fileLogDataChatBot.set(`${msg.chat.id} || ${msg.message_id} || ${bs.message_id}`)
            //let dbs = await fileLogDataChatBot.findAll()
            console.log('DATA BOT SEND MSG', bs.message_id, bs.from.id, groupSupportTarget)
            /*if(msg.from.id == groupSupport){
                let bsS = await bot.sendMessage(groupSupportVh, rContent, rOption)
                            await fileLogDataChatBot.set(`${msg.chat.id} || ${msg.message_id} || ${bsS.message_id}`)
            }*/
            if(_connection){
                _connection.write(rContent)
            }
        }
        //else bot.sendMessage(config.external.chat_id_telegram_health_check, rContent, { parse_mode: 'HTML' })
    });

    }
    catch(err){
        console.log('receiveMsg err', err)
    }
}


export const downloadFileFromMessageTele = async (bot, msg = {}) => {
  let fileId = "";
  let fileName = "";
  let mimeType = "";
  if (msg.photo) {
    const photos = msg?.photo;
    if (!photos?.length) return;
    const maxSizePhoto = photos.pop();
    fileId = maxSizePhoto.file_id;
  }
  if (msg.document) {
    fileId = msg.document.file_id;
    fileName = msg.document.file_name;
    mimeType = msg.document.mime_type;
  }
  if (msg.video) {
    fileId = msg.video.file_id;
    fileName = msg.video.file_name || "video.mp4";
    mimeType = msg.video.mime_type || "video/mp4";
  }
  const profile = await bot.getFile(fileId);
  if (!fileName) {
    fileName = profile.file_name;
  }
  console.log("profile", profile);
  const takeFile = await Axios.get(
    `https://api.telegram.org/file/bot${config.external.token_telegram_chatbot}/${profile?.file_path}`,
    { responseType: "arraybuffer" }
  );
  // fs.writeFileSync("tele/" + profile.file_path, takeFile.data);
  return {
    dataBuffer: takeFile.data,
    fileName: fileName,
  };
};

export function throttleByKey(exeFunction, delay) {
  const cache = {};
  return function (key, ...args) {
    if (!cache[key]) {
      cache[key] = {
        callback: null,
        data: [],
      };
    }
    cache[key].data.push(...args);
    if (!cache[key].callback) {
      cache[key].callback = setTimeout(() => {
        try {
          exeFunction.apply(this, cache[key].data);
          delete cache[key];
        } catch (err) {
          console.log(
            "file: utils.js:14 ~ lastCall[key]=setTimeout ~ err:",
            exeFunction?.name,
            err
          );
        }
      }, delay);
    }
  };
}

const throttleImageForGroup = throttleByKey(async (...data) => {
  console.log("data.length", data.length);
  let inputMedia = []
  let c = null
  for(let i = 0; i < data.length; i++){
    if(!c) c = data[i].caption
    let ii = {
        type: "photo",
            media: data[i].photo.dataBuffer,
    }
    if(i == (data.length - 1)) ii.caption = c//data[i].caption
    inputMedia.push(ii)
  }
  let r = await data[0].bot.sendMediaGroup(
    data[0].chatId,
    /*data.map((d) => {
      return {
        type: "photo",
        media: d.photo.dataBuffer,
        caption: d.caption,
    parse_mode: "HTML"
      };
    })*/
    inputMedia,
    data[0].option
  );
  for(let i = 0; i < r.length; i++){
    if(data[i] && data[i].msg){
         await fileLogDataChatBot.set(`${data[i].msg.chat_id} || ${data[i].msg.message_id} || ${r[i].message_id}`)
    }
  }
  //await fileLogDataChatBot.set(`${data[0].msg.chat_id} || ${data[0].msg.message_id} || ${r.message_id}`)
}, 2000);

const moment = require('moment')
function getConvertDateNumber() {
  const currentTime = moment();
  const day = moment().format('YYMMDD');
  const roundedTime = currentTime.startOf('minute');
  const remainder = Math.floor(roundedTime.minute() / 5) * 5;
  return Number(roundedTime.minute(remainder).format('HHmm')) + Number(day);
}
function decodeToOTP(otp) {
  return Number(otp) - getConvertDateNumber();
}

function getOrderCode(text){
    const regex = /(?<=DH)(\d{6})/g;
    const matches = text.match(regex);
    if(!matches){
      return []
    }
    const out = matches.map(m=>"DH"+m)
    return out
}

const getUserNameByOrder = async function(orderCode){
    let orderObj = await models.Order.findOne({
                where: {
                        code: orderCode
                },
                raw: true
        })
    if(!orderObj) return null
    let orderCreatedBy = orderObj.createdBy
    let userObj = await models.User.findOne({
        attributes: ['username'],
        where: {id: orderCreatedBy},
        raw: true
    })
    if(!userObj) return null
    return userObj.username
}

const reRenderContent = async function(text){
    let orderCode = getOrderCode(text)
    if(!orderCode.length) return text
    for(let o of orderCode){
        // find user here
        let orderObj = await models.Order.findOne({
        where: {
            code: o
        },
        raw: true
    })
    let orderCreatedBy = orderObj ? orderObj.createdBy : null
    let orderCreatedByTele = null
    if(orderCreatedBy){
        for(let _chatId in dataInit.user){
            if(dataInit.user[_chatId].userId == orderCreatedBy){
                orderCreatedByTele = dataInit.user[_chatId].teleNameCode
            }
        }
    }
    if(orderCreatedByTele){
        text = text.replace(o, ('@' + orderCreatedByTele + ' ' + o))
    }
    }
    return text
}

function setQueueChatPrivate(chatId, value){
    queueChatPrivate[chatId] = value
    queueChatPrivateTime[chatId] = Date.now()
}

/**
 * Gửi tin nhắn hàng loạt đến danh sách giáo viên
 * @param {Object} bot - Bot instance từ TelegramBot
 * @param {string} message - Nội dung tin nhắn cần gửi
 * @param {Array<string>} teacherCodes - Danh sách mã giáo viên (username)
 * @param {Object} options - Tùy chọn: parse_mode, delay giữa các tin nhắn (ms)
 * @returns {Promise<Object>} Kết quả gửi tin nhắn: { success: [], failed: [], total: number }
 */
export async function sendBulkMessageToTeachers(bot, message, teacherCodes, options = {}) {
    const { parse_mode = 'HTML', delay = 300 } = options;
    const result = {
        success: [],
        failed: [],
        total: teacherCodes.length
    };
    if (!bot || !message || !Array.isArray(teacherCodes) || teacherCodes.length === 0) {
        console.error('sendBulkMessageToTeachers: Tham số không hợp lệ');
        return result;
    }
    for (let i = 0; i < teacherCodes.length; i++) {
        const teacherCode = teacherCodes[i].trim();
        if (!teacherCode) {
            result.failed.push({
                teacherCode: teacherCode,
                reason: 'Mã giáo viên rỗng'
            });
            continue;
        }
        const userProfile = dataInit.userByUserName[teacherCode];
        if (!userProfile || !userProfile.teleChatId) {
            result.failed.push({
                teacherCode: teacherCode,
                reason: 'Không tìm thấy giáo viên hoặc giáo viên chưa đăng ký Telegram'
            });
            continue;
        }
        try {
            await bot.sendMessage(userProfile.teleChatId, message, { parse_mode });
            result.success.push({
                teacherCode: teacherCode,
                teleChatId: userProfile.teleChatId,
                userName: userProfile.userName
            });
            console.log(`Đã gửi tin nhắn thành công đến giáo viên ${teacherCode} (${userProfile.userName})`);
        } catch (error) {
            result.failed.push({
                teacherCode: teacherCode,
                teleChatId: userProfile.teleChatId,
                userName: userProfile.userName,
                reason: error.message || 'Lỗi không xác định'
            });
            console.error(`Lỗi khi gửi tin nhắn đến giáo viên ${teacherCode}:`, error.message);
        }
        if (i < teacherCodes.length - 1) {
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
    console.log(`Hoàn thành gửi tin nhắn hàng loạt: ${result.success.length}/${result.total} thành công, ${result.failed.length} thất bại`);
    return result;
}

const getIndexQuestion = async (text) => {
  try {
    const apiDomain = 'http://192.168.12.42:8000'
    const response = await axios.post(apiDomain + "/indexQ", {
      q: text
    }, {
      headers: {
        "Content-Type": "application/json"
      }
    });
    console.log(response.data);
    let result = response.data//'0'
    /*if(response.data){
        let resCode = response.data.substring(0, 2)//+response.data.substring(2) || +response.data.substring(1)
        if(dataInit.teacherSupportAnswer[resCode]){
            result = dataInit.teacherSupportAnswer[resCode]
        }
    }*/
        return result;
  } catch (error) {
    console.error("Error fetching data:", error);
    throw error;
  }
};

const startBot = async() => {
    const token = config.external.token_telegram_chatbot;
    const TelegramBot = require('node-telegram-bot-api');
    const tBot = new TelegramBot(token, { polling: true });
    hello(tBot)
    //receiveMsg(tBot)
}
//startBot()
var _connection = null
var _net = require('net');
var _server = _net.createServer(function(connection) {
       _connection = connection
       console.log('client connected');
       connection.on('data', async function(data){
        data = data.toString()
        console.log('data', data)
        try{
            data = JSON.parse(data)
            if(data.type && data.type == 'hello'){
                console.log('Client say', data.text)
            }
            else if(data.type == 'dataInit.user'){
                connection.write(JSON.stringify({
                    type: 'dataInit.user',
                    data: dataInit.user
                }))
            }
            else if(data.type == 'dataInit.threadsCSHV'){
                let threads = await fileLogDataTopic.findAll()
                connection.write(JSON.stringify({
                                                            type: 'dataInit.threadsCSHV.r',
                                                            data: threads
                                                    }))
            }
            else if(data.type == 'dataInit.threadsCSHV.create'){
                await fileLogDataTopic.set(`${data.data.username} || ${data.data.teleGroupId} || ${data.data.message_thread_id}`)
                connection.write(JSON.stringify({
                    type: 'dataInit.threadsCSHV.create.r'

                }))
            }
            else if(data.type == 'CSHV.findToReply'){
                let msgReply = await fileDataMsgReplyCshv.findAll()
                let dbsFound = false
                for(let msgReplyItem of msgReply){
                    //if(msgReplyItem.botMsgId == data.data.reply_message_id && msgReplyItem.chatId == data.data.chatId){
                    if(msgReplyItem.targetId == data.data.orgChatId && msgReplyItem.chatId == data.data.targetId && msgReplyItem.botMsgId == data.data.reply_message_id){
                                        //if(data.data.reply_message_id == msgReplyItem.botMsgId){
                            dbsFound = msgReplyItem.orgMsgId
                            break
                    }
                    else if(msgReplyItem.chatId == data.data.orgChatId && msgReplyItem.targetId == data.data.targetId && msgReplyItem.orgMsgId == data.data.reply_message_id){
                            dbsFound = msgReplyItem.botMsgId
                            break
                    }


                }
                connection.write(JSON.stringify({
                    type: 'CSHV.findToReply.r',
                    data: data.data,
                    orgMsgId: dbsFound
                }))
            }
            else if(data.type == 'CSHV.findToReply.create'){
                await fileDataMsgReplyCshv.set(`${data.data.chatId} || ${data.data.orgMsgId} || ${data.data.botMsgId} || ${data.data.targetId}`)
            }
            else if(data.type == 'decodeToOTP' && data.data){
                let dataResult = {
                    type: data.type + '.r'
                }
                let existUser = await models.UserProfile.findOne({
                                                            where: { teleChatId: data.data.teleChatId },
                                                            raw: true
                                                    })
                if(existUser){
                    dataResult.data = existUser
                }
                else{
                    let isOtpUsed = false
                    let otpUser = await models.UserProfile.findOne({
                        where: { teleOtp: data.data.text },
                        raw: true
                    })
                    let userId = decodeToOTP(data.data)
                    if(otpUser || !userId){
                        dataResult.error = true
                        dataResult.message = "Mã đăng ký không hợp lệ, vui lòng nhập lại!",
                        dataResult.teleChatId = data.data.teleChatId
                    }
                    else if(userId){
                        let userObj = await models.User.findOne({
                            where: {id: userId},
                            raw: true
                        })
                        if(userObj){
                            await models.UserProfile.create({
                                                                                        userId: userObj.id,
                                                                                        userName: userObj.username,
                                                                                        teleNameCode: data.data.teleNameCode,
                                                                                        teleChatId: data.data.teleChatId,
                                                                                        teleOtp: data.data.text
                                                                                })
                            dataResult.data = {
                                userId: userObj.id, userName: userObj.username, teleNameCode: data.data.teleNameCode, teleChatId: data.data.teleChatId
                            }
                        }
                    }
                }
                if(!dataResult.data){
                    dataResult.error = true
                    dataResult.message = "Mã đăng ký không hợp lệ, vui lòng nhập lại!"
                    dataResult.teleChatId = data.data.teleChatId
                }
                connection.write(JSON.stringify(dataResult))
            }
            else if(data.type == 'sendBulkMessageToTeachers' && data.data){
                let dataResult = {
                    type: data.type + '.r'
                }
                try {
                    if (!globalBotInstance) {
                        dataResult.error = true
                        dataResult.message = 'Bot chưa được khởi tạo'
                    }
                    else if (!data.data.message || !data.data.teacherCodes || !Array.isArray(data.data.teacherCodes)) {
                        dataResult.error = true
                        dataResult.message = 'Tham số không hợp lệ: cần có message và teacherCodes (mảng)'
                    }
                    else {
                        const sendResult = await sendBulkMessageToTeachers(
                            globalBotInstance,
                            data.data.message,
                            data.data.teacherCodes,
                            {
                                parse_mode: data.data.parse_mode || 'HTML',
                                delay: data.data.delay || 300
                            }
                        )
                        dataResult.data = sendResult
                        dataResult.message = `Đã gửi ${sendResult.success.length}/${sendResult.total} tin nhắn thành công`
                    }
                } catch (error) {
                    dataResult.error = true
                    dataResult.message = error.message || 'Lỗi không xác định khi gửi tin nhắn hàng loạt'
                    console.error('Lỗi sendBulkMessageToTeachers:', error)
                }
                connection.write(JSON.stringify(dataResult))
            }
            else if(data.type == '' && data.data){
                let isOtpUsed = false
            }
        }
        catch(e){
            console.log('e12', e)
        }
        return
       })
       connection.on('end', function() {
                 console.log('client disconnected');
              });

       //connection.write('Hello World!\r\n');
       //connection.pipe(connection);
});

_server.listen(3036, function() {
       console.log('server is listening');
});

async function sendHeartbeat() {
  await axios.post("http://10.20.139.66:3001/publish", {
    type: 'bot_teacher_heartbeat',
    ts: Date.now()
  }, {
      headers: {
        "Content-Type": "application/json"
      }
    });
}
// gửi heartbeat mỗi 3 giây
setInterval(sendHeartbeat, 3000);