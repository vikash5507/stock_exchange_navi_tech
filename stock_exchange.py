import sys
from heapq import heappush, heappop, heapreplace

# Global vars to store buyMaxHeap and sellMinHeap
buyHeap, sellHeap = {}, {}

def execute_trade(stock):
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

    if stock not in buyHeap or stock not in sellHeap:
        # Trade Execution Not possible
        return
    
    while buyHeap[stock] and sellHeap[stock]:
        bPrice, bTime, bQty, bId = buyHeap[stock][0]
        #convert back to original buying price from Buy Max Heap
        bPrice = -bPrice
        
        sPrice, sTime, sQty, sId = sellHeap[stock][0]

        # Trade Matching Rule Failed
        if sPrice > bPrice:
            break
        
        heappop(sellHeap[stock])
        heappop(buyHeap[stock])

        tradeQty = min(bQty, sQty)
        qtyDiff = bQty-sQty

        #update buyHeap (MaxHeap) and sellheap (MinHeap)
        if qtyDiff > 0:
            #update buyHeap with remaing qty
            bPrice = -bPrice
            
            bQty = qtyDiff
            data = (bPrice, bTime, bQty, bId)
            heappush(buyHeap[stock], data)
        elif qtyDiff < 0:
            sQty = -qtyDiff
            data = (sPrice, sTime, sQty, sId)
            heappush(sellHeap[stock], data)

        sPrice = "{0:.2f}".format(sPrice)
        consoleStream = bId+"  "+sPrice+"  "+str(tradeQty)+"  "+sId
        print(consoleStream)


def process_trade(trade):
    """
        process each trade stock and add it to respective data structure 
        - (dictionary handle the mapping of different stocks to its buyMaxHeap and sellMinHeap )
        - (max-heap handles Buy Orders and min-heap handles Sell Orders)

        Parameters
        ---------
        trade : list 
            containing 6 elements [ordId, time, stockName, buy/sell, quantity, id]

        Returns
        ------
        stockName: str 
            processed stockName trade data, by converting trade list data to TUPLES to handle MaxHeap and MinHeap Operations
    """
    ordId, time, stockName, ops, price, qty = trade[0], trade[1], trade[2], trade[3], trade[4], trade[5]

    #data conversion for time, price, qty for priority comparison
    time = int(''.join(time.split(':')))
    price = float(price)
    qty = int(qty)

    if ops == 'sell':

        data = (price, time, qty, ordId)

        if stockName not in sellHeap:
            sellHeap[stockName] = []
        heappush(sellHeap[stockName], data)

    elif ops == 'buy':
        # price value changed to -ve to convert python default Min Heap to Max heap, time value won't change as its priority is same
        data = (-price, time, qty, ordId)

        if stockName not in buyHeap:
            buyHeap[stockName] = []
        heappush(buyHeap[stockName], data)
        
    else:
        print("Exception Log: Not a valid operation !! Picking up next trade", ops)
        raise ValueError("Not a valid entry/operation")
    
    return stockName
    

def process(inputFile):
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
                    stock = process_trade(row)
                    execute_trade(stock)
                except:
                    print("Error: Not a valid operation!!")
    except FileNotFoundError as fnfError:
        print("File Not found!!", fnfError)

            

def main():
    inputFile = sys.argv[1]
    try:
        process(inputFile)
    except:
        print("Error: Not a valid input!!")

if __name__ == "__main__":
    main()
