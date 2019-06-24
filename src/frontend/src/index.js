import React from 'react';
import ReactDOM from 'react-dom';
import CanvasJSReact from './canvasjs.react.js';
import './canvasjs.min.js';
import './style.css';
// import Home from './Home'
// import './config'
import {
  firebaseConnect,
  isLoaded,
  isEmpty
} from 'react-redux-firebase'

import firebase from 'firebase/app'
import 'firebase/auth'
import 'firebase/database'
import 'firebase/firestore' // make sure you add this for firestore
import moment from 'moment';

var Component = React.Component;
var CanvasJS = CanvasJSReact.CanvasJS;
var CanvasJSChart = CanvasJSReact.CanvasJSChart;

var state_tmp = [];
var globaltime = [];
var globalresult = [];
var globalpredict = [];
var config = {
    apiKey: "AIzaSyBK1dWKutfGcyiQ1cB296b3-cnMZkaNe-M",
    authDomain: "cloud-computing-final-6bc36.firebaseapp.com",
    databaseURL: "https://cloud-computing-final-6bc36.firebaseio.com",
    projectId: "cloud-computing-final-6bc36",
    storageBucket: "cloud-computing-final-6bc36.appspot.com",
    messagingSenderId: "524632189425",
    appId: "1:524632189425:web:208cfb0fa1fa2876"
};
var date = '';
var length = 0;

firebase.initializeApp(config);


class Chart extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            datas : []
        }
    }

    updateState = (snapshot) => {
        var datas = snapshot;
        var new_state = [];
        snapshot.forEach(function(childSnapshot) {
            var timestamp = childSnapshot.val()['date'];
            var time = moment(timestamp,'YYYY-MM-DD HH:mm:ss');
            // var time2 = moment(timestamp).format('YYYY-MM-DD HH:mm:ss');
            // console.log(timestamp);
            // var year = time[0];
            // var month = time[1];
            // var day = time[2];
            // var hour = time[3];

            new_state.push({
                x : time.toDate(),
                y : childSnapshot.val()['price']
            });
        });
        console.log(new_state);
        this.setState({
            datas : new_state
        });
    }

    componentDidMount() {
        const airdatas = firebase.database().ref('tech');
        var new_state = [];
        airdatas.on('value', this.updateState);
    }

    render() {
		const options = {
			animationEnabled: true,
			exportEnabled: true,
			theme: "light2", // "light1", "dark1", "dark2"
			title:{
				text: "TSMC stock price"
			},
			axisY: {
				title: "price",
				includeZero: false,
				suffix: ""
			},
			axisX: {
				title: "time",
				valueFormatString: "hh:mm"
			},
			data: [{
				type: "line",
				toolTipContent: "Date {x}: {y}",
				dataPoints: this.state.datas
			}]
		}
		return (
		<div id="chartContainer">
			<CanvasJSChart options = {options}
				/* onRef={ref => this.chart = ref} */
			/>
			{/*You can get reference to the chart instance as shown above using onRef. This allows you to access all chart properties and methods*/}
            <Table type='tech' master='true'
            />
            <Table type='etf'
            />
            <Table type='rand'
            />
		</div>
		);
	}
}

class Table extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            results : [],
            predicts : [],
            dates : [],
            plength : 0,
        }
    }

    updateState = (snapshot) => {
        var datas = snapshot;
        // var new_state = [];
        var new_predict = [];
        var new_result = [];
        var new_date = [];
        snapshot.forEach(function(childSnapshot) {
            var predict = childSnapshot.val()['predict'];
            var result = childSnapshot.val()['result'];
            var date = childSnapshot.val()['date'].split(' ')[1].split(':');
            date = date[0]+':'+date[1];
            if (result == 1) {
                new_result.push('漲');
            } else{
                new_result.push('跌');
            }
            if (predict == 1) {
                new_predict.push('漲');
            } else{
                new_predict.push('跌');
            }
            new_date.push(date);
        });
        this.setState({
            dates : new_date,
            predicts : new_predict,
            results : new_result,
            plength : new_predict.length
        });
        if (this.props.master == 'true'){
            globaltime = new_date;
            globalresult = new_result;
            length = globalresult.length;
        }
        // console.log(this.state.datas[0]['date']);
    }

    componentDidMount() {
        const airdatas = firebase.database().ref(this.props.type);
        var new_state = [];
        airdatas.on('value', this.updateState);
    }

    render() {
        const d = this.state.predicts;
        console.log(this.state);
        return (
        <div id="table">
            <h1>{this.props.type}</h1>
            <table className='table'>
                <tr className='tr'>
                    <th className='th'>時間</th>
                    <th className='th'>{globaltime[length-7]}</th>
                    <th className='th'>{globaltime[length-6]}</th>
                    <th className='th'>{globaltime[length-5]}</th>
                    <th className='th'>{globaltime[length-4]}</th>
                    <th className='th'>{globaltime[length-3]}</th>
                    <th className='th'>{globaltime[length-2]}</th>
                    <th className='th'>{globaltime[length-1]}</th>
                </tr>
                <tr className='tr'>
                    <td className='td'>實際</td>
                    <td className='td'>{globalresult[length-7]}</td>
                    <td className='td'>{globalresult[length-6]}</td>
                    <td className='td'>{globalresult[length-5]}</td>
                    <td className='td'>{globalresult[length-4]}</td>
                    <td className='td'>{globalresult[length-3]}</td>
                    <td className='td'>{globalresult[length-2]}</td>
                    <td className='td'>--</td>
                </tr>
                <tr className='tr'>
                    <td className='td'>預測</td>
                    <td className='td'>{this.state.predicts[this.state.plength-7]}</td>
                    <td className='td'>{this.state.predicts[this.state.plength-6]}</td>
                    <td className='td'>{this.state.predicts[this.state.plength-5]}</td>
                    <td className='td'>{this.state.predicts[this.state.plength-4]}</td>
                    <td className='td'>{this.state.predicts[this.state.plength-3]}</td>
                    <td className='td'>{this.state.predicts[this.state.plength-2]}</td>
                    <td className='td'>{this.state.predicts[this.state.plength-1]}</td>
                </tr>
            </table>
        </div>
        );
    }
}
// ========================================

ReactDOM.render(
    <Chart />,
    // <Chart />,
    document.getElementById('root')
);
