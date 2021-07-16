/**
 * Black-Scholes option pricing formula and supporting statistical functions.
 * @module black-scholes
 * @author Matt Loppatto <mattloppatto@gmail.com>
 * @copyright 2014 Matt Loppatto
 */

/**
 * Standard normal cumulative distribution function.  The probability is estimated
 * by expanding the CDF into a series using the first 100 terms.
 * See {@link http://en.wikipedia.org/wiki/Normal_distribution#Cumulative_distribution_function|Wikipedia page}.
 *
 * @param {Number} x The upper bound to integrate over.  This is P{Z <= x} where Z is a standard normal random variable.
 * @returns {Number} The probability that a standard normal random variable will be less than or equal to x
 */
function stdNormCDF(x)
{
  var probability = 0;
  // avoid divergence in the series which happens around +/-8 when summing the
  // first 100 terms
  if(x >= 8)
  {
    probability = 1;
  }
  else if(x <= -8)
  {
    probability = 0;
  }
  else
  {
    for(var i = 0; i < 100; i++)
    {
      probability += (Math.pow(x, 2*i+1)/_doubleFactorial(2*i+1));
    }
    probability *= Math.pow(Math.E, -0.5*Math.pow(x, 2));
    probability /= Math.sqrt(2*Math.PI);
    probability += 0.5;
  }
  return probability;
}

/**
 * Double factorial.  See {@link http://en.wikipedia.org/wiki/Double_factorial|Wikipedia page}.
 * @private
 *
 * @param {Number} n The number to calculate the double factorial of
 * @returns {Number} The double factorial of n
 */
function _doubleFactorial(n)
{
  var val = 1;
  for(var i = n; i > 1; i-=2)
  {
    val *= i;
  }
  return val;
}

/**
 * Black-Scholes option pricing formula.
 * See {@link http://en.wikipedia.org/wiki/Black%E2%80%93Scholes_model#Black-Scholes_formula|Wikipedia page}
 * for pricing puts in addition to calls.
 *
 * @param   {Number} s       Current price of the underlying
 * @param   {Number} k       Strike price
 * @param   {Number} t       Time to experiation in years
 * @param   {Number} v       Volatility as a decimal
 * @param   {Number} r       Anual risk-free interest rate as a decimal
 * @param   {String} callPut The type of option to be priced - "call" or "put"
 * @returns {Number}         Price of the option
 */
function blackScholes(s, k, t, v, r, callPut,d=0)
{
  var price = null;
  var w = (r * t + Math.pow(v, 2) * t / 2 - Math.log(k / s)) / (v * Math.sqrt(t));
  if (callPut === "call" ||callPut==="Call")
  {
    price = s * Math.pow(Math.E,-1 * d * t) * stdNormCDF(w) - k * Math.pow(Math.E, -1 * r * t) * stdNormCDF(w - v * Math.sqrt(t));
  }
  else // put
  {
    price = k * Math.pow(Math.E, -1 * r * t) * stdNormCDF(v * Math.sqrt(t) - w) - s * Math.pow(Math.E,-1 * d * t) * stdNormCDF(-w);
  }
  return price;
}

/**
 * Calcuate omega as defined in the Black-Scholes formula.
 *
 * @param   {Number} s Current price of the underlying
 * @param   {Number} k Strike price
 * @param   {Number} t Time to experiation in years
 * @param   {Number} v Volatility as a decimal
 * @param   {Number} r Anual risk-free interest rate as a decimal
 * @returns {Number} The value of omega
 */
function getW(s, k, t, v, r)
{
  var w = (r * t + Math.pow(v, 2) * t / 2 - Math.log(k / s)) / (v * Math.sqrt(t));
  return w;
}


/**
 * Determine implied volatility of options based on their prices.
 * @module implied-volatility
 * @author Matt Loppatto
 * @copyright 2014 Matt Loppatto
 */

/*var bs = require("black-scholes"); */

/**
 * Calculate a close estimate of implied volatility given an option price.  A
 * binary search type approach is used to determine the implied volatility.
 *
 * @param {Number} expectedCost The market price of the option
 * @param {Number} s Current price of the underlying
 * @param {Number} k Strike price
 * @param {Number} t Time to experiation in years
 * @param {Number} r Anual risk-free interest rate as a decimal
 * @param {String} callPut The type of option priced - "call" or "put"
 * @param {Number} [estimate=.1] An initial estimate of implied volatility
 * @returns {Number} The implied volatility estimate
 */
function getImpliedVolatility(expectedCost, s, k, t, r, callPut, d,estimate)
{
  estimate = estimate || .2;
  var low = 0;
  var high = Infinity;
  // perform 100 iterations max
  for(var i = 0; i < 100; i++)
  {
    var actualCost = blackScholes(s, k, t, estimate, r, callPut,d);
    // compare the price down to the cent
    if(expectedCost * 100 == Math.floor(actualCost * 100))
    {
      break;
    }
    else if(actualCost > expectedCost)
    {
      high = estimate;
      estimate = (estimate - low) / 2 + low
    }
    else
    {
      low = estimate;
      estimate = (high - estimate) / 2 + estimate;
      if(!isFinite(estimate)) estimate = low * 2;
    }
  }
  return estimate;
}



/*
module.exports = {
  blackScholes: blackScholes,
  stdNormCDF: stdNormCDF,
  getW: getW
};
*/

/**
 * Calculation of option greeks.
 * See {@link http://en.wikipedia.org/wiki/Black%E2%80%93Scholes_model#The_Greeks|Wikipedia}
 * @module greeks
 * @author Matt Loppatto
 * @copyright 2014 Matt Loppatto
 */

/**
 * Standard normal density function.
 *
 * @private
 * @param {Number} x The value to calculate the standard normal density of
 * @returns {Number} The value of the standard normal density function at x
 */
function _stdNormDensity(x)
{
  return Math.pow(Math.E, -1 * Math.pow(x, 2) / 2) / Math.sqrt(2 * Math.PI);
}

/**
 * Calculates the delta of an option.
 *
 * @param {Number} s Current price of the underlying
 * @param {Number} k Strike price
 * @param {Number} t Time to experiation in years
 * @param {Number} v Volatility as a decimal
 * @param {Number} r Anual risk-free interest rate as a decimal
 * @param {String} callPut The type of option - "call" or "put"
 * @returns {Number} The delta of the option
 */
function getDelta(s, k, t, v, r, callPut)
{
  if(callPut === "call" || callPut==='Call')
  {
    return _callDelta(s, k, t, v, r);
  }
  else // put
  {
    return _putDelta(s, k, t, v, r);
  }
}

/**
 * Calculates the delta of a call option.
 *
 * @private
 * @param {Number} s Current price of the underlying
 * @param {Number} k Strike price
 * @param {Number} t Time to experiation in years
 * @param {Number} v Volatility as a decimal
 * @param {Number} r Anual risk-free interest rate as a decimal
 * @returns {Number} The delta of the call option
 */
function _callDelta(s, k, t, v, r)
{
  var w = getW(s, k, t, v, r);
  var delta = null;
  if(!isFinite(w))
  {
    delta = (s > k) ? 1 : 0;
  }
  else
  {
    delta = stdNormCDF(w);
  }
  return delta;
}

/**
 * Calculates the delta of a put option.
 *
 * @private
 * @param {Number} s Current price of the underlying
 * @param {Number} k Strike price
 * @param {Number} t Time to experiation in years
 * @param {Number} v Volatility as a decimal
 * @param {Number} r Anual risk-free interest rate as a decimal
 * @returns {Number} The delta of the put option
 */
function _putDelta(s, k, t, v, r)
{
  var delta = _callDelta(s, k, t, v, r) - 1;
  return (delta == -1 && k == s) ? 0 : delta;
}

/**
 * Calculates the rho of an option.
 *
 * @param {Number} s Current price of the underlying
 * @param {Number} k Strike price
 * @param {Number} t Time to experiation in years
 * @param {Number} v Volatility as a decimal
 * @param {Number} r Anual risk-free interest rate as a decimal
 * @param {String} callPut The type of option - "call" or "put"
 * @param {String} [scale=100] The value to scale rho by (100=100BPS=1%, 10000=1BPS=.01%)
 * @returns {Number} The rho of the option
 */
function getRho(s, k, t, v, r, callPut, scale)
{
  scale = scale || 100;
  if(callPut === "call" || callPut==="Call")
  {
    return _callRho(s, k, t, v, r) / scale;
  }
  else // put
  {
    return _putRho(s, k, t, v, r) / scale;
  }
}

/**
 * Calculates the rho of a call option.
 *
 * @private
 * @param {Number} s Current price of the underlying
 * @param {Number} k Strike price
 * @param {Number} t Time to experiation in years
 * @param {Number} v Volatility as a decimal
 * @param {Number} r Anual risk-free interest rate as a decimal
 * @returns {Number} The rho of the call option
 */
function _callRho(s, k, t, v, r)
{
  var w = getW(s, k, t, v, r);
  if(!isNaN(w))
  {
    return k * t * Math.pow(Math.E, -1 * r * t) * stdNormCDF(w - v * Math.sqrt(t));
  }
  else
  {
    return 0;
  }
}

/**
 * Calculates the rho of a put option.
 *
 * @private
 * @param {Number} s Current price of the underlying
 * @param {Number} k Strike price
 * @param {Number} t Time to experiation in years
 * @param {Number} v Volatility as a decimal
 * @param {Number} r Anual risk-free interest rate as a decimal
 * @returns {Number} The rho of the put option
 */
function _putRho(s, k, t, v, r)
{
  var w = getW(s, k, t, v, r);
  if(!isNaN(w))
  {
    return -1 * k * t * Math.pow(Math.E, -1 * r * t) * stdNormCDF(v * Math.sqrt(t) - w);
  }
  else
  {
    return 0;
  }
}

/**
 * Calculates the vega of a call and put option.
 *
 * @param {Number} s Current price of the underlying
 * @param {Number} k Strike price
 * @param {Number} t Time to experiation in years
 * @param {Number} v Volatility as a decimal
 * @param {Number} r Anual risk-free interest rate as a decimal
 * @returns {Number} The vega of the option
 */
function getVega(s, k, t, v, r)
{
  var w = getW(s, k, t, v, r);
  return (isFinite(w)) ? (s * Math.sqrt(t) * _stdNormDensity(w) / 100) : 0;
}

/**
 * Calculates the theta of an option.
 *
 * @param {Number} s Current price of the underlying
 * @param {Number} k Strike price
 * @param {Number} t Time to experiation in years
 * @param {Number} v Volatility as a decimal
 * @param {Number} r Anual risk-free interest rate as a decimal
 * @param {String} callPut The type of option - "call" or "put"
 * @param {String} [scale=365] The number of days to scale theta by - usually 365 or 252
 * @returns {Number} The theta of the option
 */
function getTheta(s, k, t, v, r, callPut, scale)
{
  scale = scale || 365;
  if(callPut === "call" || callPut ==="Call")
  {
    return _callTheta(s, k, t, v, r) / scale;
  }
  else // put
  {
    return _putTheta(s, k, t, v, r) / scale;
  }
}

/**
 * Calculates the theta of a call option.
 *
 * @private
 * @param {Number} s Current price of the underlying
 * @param {Number} k Strike price
 * @param {Number} t Time to experiation in years
 * @param {Number} v Volatility as a decimal
 * @param {Number} r Anual risk-free interest rate as a decimal
 * @returns {Number} The theta of the call option
 */
function _callTheta(s, k, t, v, r)
{
  var w = getW(s, k, t, v, r);
  if(isFinite(w))
  {
    return -1 * v * s * _stdNormDensity(w) / (2 * Math.sqrt(t)) - k * r * Math.pow(Math.E, -1 * r * t) * stdNormCDF(w - v * Math.sqrt(t));
  }
  else
  {
    return 0;
  }
}

/**
 * Calculates the theta of a put option.
 *
 * @private
 * @param {Number} s Current price of the underlying
 * @param {Number} k Strike price
 * @param {Number} t Time to experiation in years
 * @param {Number} v Volatility as a decimal
 * @param {Number} r Anual risk-free interest rate as a decimal
 * @returns {Number} The theta of the put option
 */
function _putTheta(s, k, t, v, r)
{
  var w = getW(s, k, t, v, r);
  if(isFinite(w))
  {
    return -1 * v * s * _stdNormDensity(w) / (2 * Math.sqrt(t)) + k * r * Math.pow(Math.E, -1 * r * t) * stdNormCDF(v * Math.sqrt(t) - w);
  }
  else
  {
    return 0;
  }
}

/**
 * Calculates the gamma of a call and put option.
 *
 * @param {Number} s Current price of the underlying
 * @param {Number} k Strike price
 * @param {Number} t Time to experiation in years
 * @param {Number} v Volatility as a decimal
 * @param {Number} r Anual risk-free interest rate as a decimal
 * @returns {Number} The gamma of the option
 */
function getGamma(s, k, t, v, r)
{
  var w = getW(s, k, t, v, r);
  return (isFinite(w)) ? (_stdNormDensity(w) / (s * v * Math.sqrt(t))) : 0;
}

