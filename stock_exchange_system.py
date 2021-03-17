import sys
from heapq import heappush, heappop, heapreplace

class stockExchange():
    """
        Class Implementation
    """
    buyHeap, sellHeap = {}, {}

    def __init__(self):
        self.buyHeap =  {}
        self.sellHeap = {}

    def execute_trade(self, stock):
        """
            Execute the trade on Matching Rule -->
            if buy_price >= sell_price

            Parameters
            --------
            stock: str
                stockName to execute trade with available buyMaxHeap and sellMinHeap data

            Returns
            ------
            void: print traded data after apply Trade Matching Rule to console, ideally will be entered into order book
        """

        if stock not in self.buyHeap or stock not in self.sellHeap:
            # Trade Execution Not possible
            return
        
        while self.buyHeap[stock] and self.sellHeap[stock]:
            bPrice, bTime, bQty, bId = self.buyHeap[stock][0]
            #convert back to original buying price from Buy Max Heap
            bPrice = -bPrice
            
            sPrice, sTime, sQty, sId = self.sellHeap[stock][0]

            # Trade Matching Rule Failed
            if sPrice > bPrice:
                break
            
            heappop(self.sellHeap[stock])
            heappop(self.buyHeap[stock])

            tradeQty = min(bQty, sQty)
            qtyDiff = bQty-sQty

            #update buyHeap (MaxHeap) and sellheap (MinHeap)
            if qtyDiff > 0:
                #update buyHeap with remaing qty
                bPrice = -bPrice
                
                bQty = qtyDiff
                data = (bPrice, bTime, bQty, bId)
                heappush(self.buyHeap[stock], data)
            elif qtyDiff < 0:
                sQty = -qtyDiff
                data = (sPrice, sTime, sQty, sId)
                heappush(self.sellHeap[stock], data)

            sPrice = "{0:.2f}".format(sPrice)
            consoleStream = bId+"  "+sPrice+"  "+str(tradeQty)+"  "+sId
            print(consoleStream)

    def preprocess_trade_data(self, trade):
        """
            return processed data to feed to buy or sell heaps
        """
        ordId, time, stockName, ops, price, qty = trade[0], trade[1], trade[2], trade[3], trade[4], trade[5]

        #data conversion for time, price, qty for priority comparison
        time = int(''.join(time.split(':')))
        price = float(price)
        qty = int(qty)

        if ops == 'sell':
            data = (price, time, qty, ordId)
        elif ops == 'buy':
            # price value changed to -ve to convert python default Min Heap to Max heap, time value won't change as its priority is same
            data = (-price, time, qty, ordId)
        else:
            print("Exception Log: Not a valid operation !! Picking up next trade", ops)
            raise ValueError("Not a valid entry/operation")
        
        return (data, ops, stockName)
    
    def feed_trade_data_to_heap(self, data, ops, stock):
        """
            feed data to heaps
        """
        if ops == 'sell':
            if stock not in self.sellHeap:
                self.sellHeap[stock] = []
            heappush(self.sellHeap[stock], data)
        elif ops == 'buy':
            if stock not in self.buyHeap:
                self.buyHeap[stock] = []
            heappush(self.buyHeap[stock], data)
        
        return stock

    
    def process(self, inputFile):
        """
            process input File to read each trade instruction

            Parameters
            --------
            inputFile: str
                inpute file path
            
            Returns
            --------
            void: process trade inputs and executes trade
        """

        try:
            with open(inputFile, 'r') as reader:
                for line in reader:
                    row = line.split()
                    if len(row) < 6:
                        print("Exception Log: Not a valid input !! Picking up next line", row)
                        raise ValueError("Invalid input!!")
                    
                    try:
                        ### process and execute trade ###
                        data, ops, stock = self.preprocess_trade_data(row)
                        stock = self.feed_trade_data_to_heap(data, ops, stock)
                        self.execute_trade(stock)
                    except:
                        print("Error: Not a valid operation!!")
        except FileNotFoundError as fnfError:
            print("File Not found!!", fnfError)
            pass


def main():
    inputFile = sys.argv[1]
    stockExchangeObj = stockExchange()
    stockExchangeObj.process(inputFile)
    

if __name__ == "__main__":
    ## Run instuction or command
    # python3 -m stock_exchange input_trades.txt
    main()