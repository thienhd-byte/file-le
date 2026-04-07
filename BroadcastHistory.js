'use strict';
const Sequelize = require('sequelize');

module.exports = (sequelize, DataTypes) => {
  const BroadcastHistory = sequelize.define('BroadcastHistory', {
    id: {
      type: DataTypes.BIGINT,
      primaryKey: true,
      autoIncrement: true,
      allowNull: false,
    },
    timestamp: {
      type: DataTypes.STRING(50),
      allowNull: false,
    },
    senderId: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },
    groupCode: {
      type: DataTypes.STRING(100),
      allowNull: true,
    },
    messageHtml: {
      type: DataTypes.LONGTEXT,
      allowNull: true,
    },
    mediaType: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },
    totalSent: {
      type: DataTypes.INTEGER,
      defaultValue: 0,
    },
    totalFailed: {
      type: DataTypes.INTEGER,
      defaultValue: 0,
    },
    broadcastId: {
      type: DataTypes.STRING(100),
      allowNull: false,
      unique: true,
    },
    chainId: {
      type: DataTypes.STRING(100),
      allowNull: true,
    },
    parentBroadcastId: {
      type: DataTypes.STRING(100),
      allowNull: true,
    },
    broadcastTitle: {
      type: DataTypes.LONGTEXT,
      allowNull: true,
    },
    messageType: {
      type: DataTypes.STRING(50),
      defaultValue: 'original',
    },
    isSurvey: {
      type: DataTypes.BOOLEAN,
      defaultValue: false,
    },
    surveyOptions: {
      type: DataTypes.LONGTEXT,
      allowNull: true,
    },
  }, {
    tableName: 'broadcast_history',
    timestamps: true,
    freezeTableName: true,
  });

  return BroadcastHistory;
};
