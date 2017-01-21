#! usr/bin/env/python
# coding=utf8

import pymssql
import datetime
import logging

logger = logging.getLogger('purchasing')
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setFormatter(formatter)
file_handler = logging.FileHandler("D:/log/purchasing.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console)


def filter_trade():
    """ filter of trade via sku statutes """
    procedure = "www_outofstock_sku '7','春节放假,清仓,停产,停售,线上清仓'"
    select_query = "select nid,ADDRESSOWNER,REASONCODE, allGoodsDetail,ordertime from P_TradeUn where PROTECTIONELIGIBILITYTYPE='缺货订单' and isnull(REASONCODE,'')  not like '%不采购%' and  isnull(REASONCODE,'') not like '%放假采不到%'"
    check_status = "select goodsid,goodsskustatus from B_goodssku where sku=%s and sku!='S49412' "
    select_purchaser = "select purchaser from B_goods where nid=%s"
    select_sku = "select sku from P_tradedtun where tradenid=%s"
    con = pymssql.connect(server='121.196.233.153', user='sa', password='allroot89739659', database='ShopElf', port='12580')
    cur = con.cursor(as_dict=True)
    cur.execute(select_query)
    trade = cur.fetchall()
    cur.execute(procedure)
    filter_trades = cur.fetchall()
    # filted_trades = [item['sku'] + str(item['nid']) for item in filter_trades]
    # print filted_trades
    for row in trade:
        cur.execute(select_sku, (row['nid'],))
        sku_list = cur.fetchall()
        sku_list = [ele['sku'] for ele in sku_list]
        trade_status = {row['nid']: u'不采购:'}
        for sku in sku_list:
            cur.execute(check_status, (sku,))
            sku_status = cur.fetchone()
            try:
                cur.execute(select_purchaser, (sku_status['goodsid'],))
                purchase_info = cur.fetchone()
                purchaser = purchase_info['purchaser']
                now = datetime.datetime.now().strftime("%m-%d")
                # festival
                if sku_status['goodsskustatus'].encode('UTF-8') == '春节放假':
                    if {'sku': sku, 'nid': row['nid']} not in filter_trades:
                        trade_status[row['nid']] = trade_status[row['nid']] + " " + purchaser + str(now) + ":" + unicode(
                               sku) + u"春节放假" + ";"

                if sku_status['goodsskustatus'].encode('UTF-8') == '停产':

                    if {'sku': sku, 'nid': row['nid']} not in filter_trades:
                        trade_status[row['nid']] = trade_status[row['nid']] + " " + purchaser + str(now) + ":" + unicode(
                               sku) + u"停产" + ";"
                elif sku_status['goodsskustatus'].encode('UTF-8') == '停售':
                    if row["ADDRESSOWNER"] == 'ebay':
                        trade_status[row['nid']] = trade_status[row['nid']] + " " + purchaser + str(now) + ":" + unicode(sku) + u"停售" + ";"
                    if row["ADDRESSOWNER"] == 'aliexpress':
                        trade_status[row['nid']] = trade_status[row['nid']] + " " + purchaser + str(now) + ":" + unicode(sku) + u"停售" + ";"

                elif sku_status['goodsskustatus'].encode('UTF-8') == '清仓':
                    if row["ADDRESSOWNER"] == 'ebay':
                        trade_status[row['nid']] = trade_status[row['nid']] + " " + purchaser + str(now) + ":" + unicode(sku) + u"清仓" + ";"

                    if row["ADDRESSOWNER"] == 'aliexpress':
                        trade_status[row['nid']] = trade_status[row['nid']] + " " + purchaser + str(now) + ":" + unicode(sku) + u"清仓" + ";"

                elif sku_status['goodsskustatus'].encode('UTF-8') == '线上清仓':
                    if row["ADDRESSOWNER"] == 'ebay':
                        trade_status[row['nid']] = trade_status[row['nid']] + " " + purchaser + str(now) + ":" + unicode(sku) + u"线上清仓" + ";"
                    if row["ADDRESSOWNER"] == 'aliexpress':
                        trade_status[row['nid']] = trade_status[row['nid']] + " " + purchaser + str(now) + ":" + unicode(sku) + u"线上清仓" + ";"
            except Exception as e:
                logger.warning('error %s %s' % (e, row['nid']))
        if trade_status[row['nid']].count(';') > 0:
            if len(sku_list) == trade_status[row['nid']].count(';'):
                if u'春节放假' in trade_status[row['nid']]:
                    reasoncode = u'3.放假采不到'
                else:
                    reasoncode = u'2.不采购'
            else:
                reasoncode =u'1.部分不采购'
            yield row['nid'], row['ADDRESSOWNER'], reasoncode, row['allGoodsDetail'], row['ordertime'], trade_status[row['nid']]
    con.close()


def update_tradeun(data):
    """ mark the trades in P_tradeun with the results of function filter_trade"""

    con = pymssql.connect(server='121.196.233.153', user='sa', charset="utf8", password='allroot89739659',
                                database='ShopElf', port='12580')
    cur = con.cursor()
    update_query = "update P_tradeun set reasoncode=%s, memo=%s where nid=%s "
    memo_query = "select isnull(memo,'') as memo from p_tradeun where nid=%s"
    check_query = "select isnull((case when ISNULL(reasoncode, '')='没有问题,可以发' then '' else reasoncode end),'') as reasoncode from P_Tradeun where nid=%s"
    try:
        cur.execute(memo_query, data[0])
        memo = cur.fetchall()[0]
        new_memo = memo[0] + data[5]
        cur.execute(check_query, data[0])
        reasoncode = cur.fetchone()[0]
        if not reasoncode:
            # if the reason code is empty or not
            cur.execute(update_query, (data[2], new_memo, data[0]))
            # print "%s: update %s" % (datetime.datetime.now(), data[0])
            logger.info("updating success %s" % (data[0]))

    except Exception as e:
        logger.warning("updating fails %s" % (data[0]))
    con.commit()
    con.close()


def mark_trade():
    start = datetime.datetime.now()
    for i in filter_trade():
        update_tradeun(i)
    end = datetime.datetime.now()
    return end - start

if __name__ == '__main__':
    mark_trade()



