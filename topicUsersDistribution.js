
// var userNum = 50000;
// var randomUserIDs = [];
// var max = 200000;
// var min = 1;
// for(var i = 0; i < userNum; i++){
// 	 randomUserIDs.push(Math.floor(Math.random() * (max - min + 1)) + min);
// }

// var cur = db.usersIDforTop10.find();
// if(cur.hasNext())
// 	var curr = cur.next();
// var randomUserIDs = curr["userIDs"];

var userNum = 50000;
var randomUserIDs = [];
for(var i = 0; i < userNum; i++) {
	randomUserIDs.push(i);
}



var mapFunction = function(){
	if(randomUserIDs.indexOf(this.idByPostNum) != -1){
		var topicParticipated = this.topicsTakeInNum_top2;
		for(var i = 0; i < topicParticipated.length; i++) {
			var topicIDNum = topicParticipated[i].split("=");
			var value = {userID: [this.idByPostNum],
						 numWith: [parseInt(topicIDNum[1])]};
			emit(topicIDNum[0], value);
		}
	}
}

var reduceFunction = function(key, values){
	var userIDArr = [];
	var numWithArr = [];
	for(var i in values){
		userIDArr = userIDArr.concat(values[i].userID) ;
		numWithArr = numWithArr.concat(values[i].numWith);
	}
	var results = {userID: userIDArr,
				   numWith: numWithArr};
	return results;
}

db.user_retweet_network_info.mapReduce(mapFunction, 
									   reduceFunction, 
									   {"out": "topicUsersDistribution_top2_default", 
									    "scope": {randomUserIDs: randomUserIDs}});

// db.createCollection("usersIDforTop5");
// db.usersIDforTop5.insert({"userIDs": randomUserIDs.sort()});