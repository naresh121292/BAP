function getDistanceFromLatLon({ lat1, lon1, lat2, lon2 }, unit = "m") {
    if (isNaN(lat1) || isNaN(lon1) || isNaN(lat2) || isNaN(lon2)) {
      return 0;
    }
    lat1 = Number(lat1);
    lon1 = Number(lon1);
    lat2 = Number(lat2);
    lon2 = Number(lon2);
    if (lat1 == 0 || lon1 == 0 || lat2 == 0 || lon2 == 0) {
      return 0;
    }
    let R = 6371; // Radius of the earth in km
    let dLat = deg2rad(lat2 - lat1); // deg2rad below
    let dLon = deg2rad(lon2 - lon1);
    let a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(deg2rad(lat1)) *
        Math.cos(deg2rad(lat2)) *
        Math.sin(dLon / 2) *
        Math.sin(dLon / 2);
    let c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    if (unit?.toLocaleLowerCase() == "km") {
      return R * c;
    } else if (unit?.toLocaleLowerCase() == "mi") {
      return (R * c) / 1.609344;
    }
    let d = R * c * 1000; // Distance in metres
    return Math.round((d + Number.EPSILON) * 100) / 100;
  }
  
  function deg2rad(deg) {
    return deg * (Math.PI / 180);
  }
  
  module.exports = {
    getDistanceFromLatLon,
  };
  