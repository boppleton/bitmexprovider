//
// [bitmexprovider] - a node API for retrieving cached data from the BitMEX API
//
//

const express = require('express')
const WebSocket = require('ws')
const ccxt = require('ccxt')
const bitmex = new ccxt.bitmex()



/////////// start data ///////////////////////////////////////

/// start bars
///
let symbols = [
    'XBTUSD',
    'ETHUSD',
]

let bars = {}
symbols.forEach((s) => {
    bars[s] = {
        m1: ['[' + s + ':m1:START]'],
        m5: ['[' + s + ':m5:START]'],
        h1: ['[' + s + ':h1:START]'],
        d1: ['[' + s + ':d1:START]']
    }
})


/////////// end start data ///////////////////////////////////////


//
///// setup api
//
const app = express()
const port = 3001
app.get('/', (req, res) => res.send('[bitmex provider v0.1] \n\n example path : /XBTUSD/m5 \n\n ' +
    'possible bins: m1, m5, h1, d1 \n\n available symbols:\n' + symbols))
Object.keys(bars).forEach(function (key) {
    app.get('/' + key + '/m1', (req, res) => res.send(bars[key].m1))
    app.get('/' + key + '/m5', (req, res) => res.send(bars[key].m5))
    app.get('/' + key + '/h1', (req, res) => res.send(bars[key].h1))
    app.get('/' + key + '/d1', (req, res) => res.send(bars[key].d1))
})
app.listen(port, () => console.log(`server started on port ${port}!`));
/////////////////////////////////////////////////////////////////////////////////////////


//
///// get initial histories
//
(async () => {
    async function getBars(symbol, bin) {
        await sleep(1000)
        console.log("getting " + symbol + bin)
        let bars1 = await bitmex.fetchOHLCV(getCCXTsymbol(symbol), bin[1] + bin[0], null, 750, {reverse: true})

        for (let i = 0; i < 1; i++) {
            let bars2 = await bitmex.fetchOHLCV(getCCXTsymbol(symbol), bin[1] + bin[0], null, 750, {
                reverse: true,
                endTime: new Date(bars1[0][0]).toISOString().split('.')[0] + "Z"
            })
            bars1 = bars2.concat(bars1)
        }

        let open = []
        let high = []
        let low = []
        let close = []
        bars1.forEach((bar, i) => {
            open.push(bar[1])
            high.push(bar[2])
            low.push(bar[3])
            close.push(bar[4])
        })
        bars[symbol][bin] = bars1
    }

    await symbols.forEach(async (s) => {
        await getBars(s, 'm1')
        await sleep(symbols.length*1000)
        await getBars(s, 'm5')
        await sleep(symbols.length*1000)
        await getBars(s, 'h1')
        await sleep(symbols.length*1000)
        await getBars(s, 'd1')
        await sleep(symbols.length*1000)
    })
})()

function getCCXTsymbol(symbol) {
    if (symbol === 'XBTUSD') {
        return 'BTC/USD'
    } else if (symbol === 'ETHUSD') {
        return 'ETH/USD'
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
}


const socketMessageListener = (event) => {

    let msg = JSON.parse(event.data)
    // console.log("msg: "+JSON.stringify(msg))

    if (msg.table === 'trade') {
        tradeMsg(msg.action, msg.data)
    }

}

const socketOpenListener = (event) => {
    console.log("bitmex ws open")
}

const socketCloseListener = (event) => {
    if (this.socket) {
        console.error('bitmex ws close')
    }
    this.socket = new WebSocket('wss://www.bitmex.com/realtime?subscribe=trade')
    this.socket.addEventListener('open', socketOpenListener)
    this.socket.addEventListener('message', socketMessageListener)
    this.socket.addEventListener('close', socketCloseListener)
}
socketCloseListener()

const tradeMsg = (action, data) => {

    if (!symbols.find((s) => {
        return s === data[0].symbol
    })) {
        return
    }

    // console.log("trade with symbol " + data[0].symbol)

    let total = 0
    data.forEach((t) => total += t.size)
    let price = data[data.length - 1].price

    setBar(data[0].symbol, 'm1')
    setBar(data[0].symbol, 'm5')
    setBar(data[0].symbol, 'h1')
    setBar(data[0].symbol, 'd1')

    async function setBar(symbol, bin) {

        let currentBar = bars[symbol][bin][bars[symbol][bin].length - 1]
        let lastbarTime = currentBar[0]
        let time = new Date(data[0].timestamp).getTime()

        if (time < lastbarTime) {
            //update current bar
            let currentBar = bars[symbol][bin][bars[symbol][bin].length - 1]

            currentBar[4] = price
            currentBar[5] += total

            if (price > currentBar[2]) {
                currentBar[2] = price
            } else if (price < currentBar[3]) {
                currentBar[3] = price
            }
        } else {
            //make new bar with this trade and last time + tim
            let barz = bars[symbol][bin]
            let close = barz[barz.length - 1][4]
            barz.push([lastbarTime + (bin === 'm1' ? 60000 : bin === 'm5' ? 300000 : bin === 'h1' ? 3600000 : 86400000), close, close, close, close, 0])
        }
    }
}
