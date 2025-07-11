function transform(line) {
    var values = line.split(',');
    var obj = new Object();
    obj.Name = values[0];
    obj.Country = values[1];
    obj.Rating = parseInt(values[2]);	
    var jsonString = JSON.stringify(obj);
    return jsonString;
}
