// @ts-check
import assert from 'node:assert';
import { createReadStream, promises as fs } from 'node:fs';
import * as csv from 'csv';

/**
 * @typedef {Object} Transaction
 * @property {string} date
 * @prop {number} [received_quantity]
 * @property {string} [received_currency]
 * @property {number} [sent_quantity]
 * @property {string} [sent_currency]
 * @property {number} [fee_amount]
 * @property {string} [fee_currency]
 * @property {string} [tag]
 * @property {string} [remark]
 *
 * @typedef {Object} TransactionPair
 * @property {string} key
 * @property {Transaction} txn
 */

const transactionsPath = './input.csv';

const TransactionOperation = {
  AIRDROP_ASSETS: 'Airdrop Assets',
  BUY: 'Buy',
  COMMISSION_HISTORY: 'Commission History',
  COMMISSION_REBATE: 'Commission Rebate',
  DEPOSIT: 'Deposit',
  DISTRIBUTION: 'Distribution',
  FEE: 'Fee',
  SELL: 'Sell',
  SMALL_ASSETS_EXCHANGE_BNB: 'Small Assets Exchange BNB',
  STAKING_REWARDS: 'Staking Rewards',
  TRANSACTION_RELATED: 'Transaction Related',
  WITHDRAW: 'Withdraw',
};

function toCointrackerDate(date) {
  const year = date.getFullYear();
  const month = `${date.getMonth() + 1}`.padStart(2, '0');
  const day = `${date.getDate()}`.padStart(2, '0');
  const hours = `${date.getHours()}`.padStart(2, '0');
  const minutes = `${date.getMinutes()}`.padStart(2, '0');
  const seconds = `${date.getSeconds()}`.padStart(2, '0');
  return `${month}/${day}/${year} ${hours}:${minutes}:${seconds}`;
}

/**
 * Retrieves a transaction from a map of transactions based on a given timestamp.
 * If an exact match is not found, it will look for transactions within +/- 1 second.
 * If no transaction is found, it creates a new transaction with the provided date and remark.
 *
 * @param {Object} params - The parameters for the function.
 * @param {number} params.seconds - The timestamp in seconds to look for.
 * @param {Map<string, Transaction>} params.transactions - A map of transactions keyed by timestamp.
 * @param {Date} params.date - The date of the transaction to create if no match is found.
 * @param {string} params.remark - The remark for the transaction to create if no match is found.
 * @return {TransactionPair} The transaction and its key
 */
function getTransaction({ seconds, transactions, date, remark }) {
  let key = seconds.toString();
  const secondsMinusOne = (seconds - 1).toString();
  const secondsPlusOne = (seconds + 1).toString();

  /**
   * Transactions don't always have the same exact second timestamp, so look for +/- 1 second.
   * @type {Transaction | undefined}
   */
  let txn;
  if (transactions.has(key)) {
    txn = transactions.get(key);
  } else if (transactions.has(secondsMinusOne)) {
    key = secondsMinusOne;
    txn = transactions.get(secondsMinusOne);
  } else if (transactions.has(secondsPlusOne)) {
    key = secondsPlusOne;
    txn = transactions.get(secondsPlusOne);
  }
  txn = txn ?? {
    date: toCointrackerDate(date),
    remark: remark,
  };

  return {
    key,
    txn,
  };
}

async function main() {
  const records = [];
  const readStream = createReadStream(transactionsPath).pipe(csv.parse());

  for await (const record of readStream) {
    records.push(record);
  }

  console.log(`Found ${records.length} records`);

  // Remove headings.
  records.shift();

  /**
   * @type {Map<string, Transaction>}
   */
  const txns = new Map();
  for (let i = 0; i < records.length; i++) {
    const [userId, utcTime, account, operation, coin, change, remark] = records[i];
    const date = new Date(utcTime);
    const seconds = Math.round(date.getTime() / 1000);
    let { key, txn } = getTransaction({ seconds, transactions: txns, date, remark });

    const debugString = `Received ${change} for ${coin} transaction on ${utcTime}.`;

    // Trading "sent" values.
    if (operation === TransactionOperation.TRANSACTION_RELATED || operation === TransactionOperation.SELL) {
      // Change should be negative because it's the money spent.
      assert(change < 0, `Trade change should be negative. ${debugString}`);
      txn = {
        ...txn,
        sent_quantity: Math.abs(change),
        sent_currency: coin,
      };
    } else if (operation === TransactionOperation.FEE) {
      // Fees
      // Fee should be negative because it's the money spent.
      assert(change < 0, `Fee change should be negative. ${debugString}`);
      txn = {
        ...txn,
        fee_amount: Math.abs(change),
        fee_currency: coin,
      };
    } else if (operation === TransactionOperation.BUY) {
      // Received value.
      assert(change > 0, `Buy change should be positive. ${debugString}`);
      txn = {
        ...txn,
        received_quantity: Math.abs(change),
        received_currency: coin,
      };
    } else if (operation === TransactionOperation.DISTRIBUTION || operation === TransactionOperation.AIRDROP_ASSETS) {
      // Airdrop.
      // assert(change > 0, `Airdrop should be positive. ${debugString}`);
      if (change < 0) {
        // I had a negative airdrop when Binance sold the delisted BCPT token.
        // Koinly shows this as a "Send", so that's what we're doing here.
        txn = {
          ...txn,
          sent_quantity: Math.abs(change),
          sent_currency: coin,
        };
      } else {
        txn = {
          ...txn,
          received_quantity: Math.abs(change),
          received_currency: coin,
          tag: 'airdrop',
        };
      }
    } else if (
      operation === TransactionOperation.STAKING_REWARDS ||
      operation === TransactionOperation.COMMISSION_HISTORY ||
      operation === TransactionOperation.COMMISSION_REBATE
    ) {
      // Staking?
      assert(change > 0, `Stake should be positive. ${debugString}`);
      txn = {
        ...txn,
        received_quantity: Math.abs(change),
        received_currency: coin,
        tag: 'staked',
      };
    } else if (operation === TransactionOperation.DEPOSIT) {
      // Deposit.
      assert(change > 0, `Deposit should be positive. ${debugString}`);
      txn = {
        ...txn,
        received_quantity: Math.abs(change),
        received_currency: coin,
      };
    } else if (operation === TransactionOperation.WITHDRAW) {
      // Withdrawal.
      assert(change < 0, `Withdrawal should be negative. ${debugString}`);
      txn = {
        ...txn,
        sent_quantity: Math.abs(change),
        sent_currency: coin,
      };
    } else if (operation === TransactionOperation.SMALL_ASSETS_EXCHANGE_BNB) {
      // These trades were all made at the same second mark, so a new key is
      // needed to differentiate them.
      key = `${key}|${remark}`;
      const storedTxn = txns.get(key);
      txn = storedTxn ?? txn;

      if (change < 0) {
        // Coin being sent.
        txn = {
          ...txn,
          sent_quantity: Math.abs(change),
          sent_currency: coin,
        };
      } else {
        // Coin being received.
        txn = {
          ...txn,
          received_quantity: Math.abs(change),
          received_currency: coin,
        };
      }
    } else {
      console.log(`ignored operation ${operation}`);
    }

    txns.set(key, txn);
  }

  console.log(`Transformed to ${txns.size} records`);

  // Map the records to the correct field names:
  const newRecords = Array.from(txns.values(), (txn) => ({
    Date: txn.date,
    'Received Quantity': txn.received_quantity,
    'Received Currency': txn.received_currency,
    'Sent Quantity': txn.sent_quantity,
    'Sent Currency': txn.sent_currency,
    'Fee Amount': txn.fee_amount,
    'Fee Currency': txn.fee_currency,
    Tag: txn.tag,
    Remark: txn.remark,
  }));

  const result = csv.stringify(newRecords, {
    header: true,
    columns: [
      'Date',
      'Received Quantity',
      'Received Currency',
      'Sent Quantity',
      'Sent Currency',
      'Fee Amount',
      'Fee Currency',
      'Tag',
      // 'Remark', // Not part of Cointracker CSV format.
    ],
  });

  await fs.writeFile('./output.csv', result);
}

main();
