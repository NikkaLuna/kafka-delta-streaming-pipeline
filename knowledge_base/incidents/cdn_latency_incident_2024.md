# CDN Latency Incident - 2024

## Incident Summary

A CDN performance issue caused slower page loads and delayed asset delivery, resulting in unusual clickstream behavior and repeated user interactions.

## Symptoms

- Increase in repeated click events
- Delayed page transitions
- Higher interaction values during affected sessions
- Users clicking the same interface elements multiple times
- Elevated anomaly scores during the latency window

## Likely User Intent

Users were attempting to navigate the site or complete actions, but slow page responses caused repeated clicks and abnormal interaction patterns.

## Business Impact

- Lower user engagement
- Increased page abandonment
- Reduced conversion rate
- Poor customer experience
- Potential increase in support complaints

## Recommended Actions

- Check CDN provider status
- Review frontend latency metrics
- Compare page load times against baseline
- Inspect error rates for static assets
- Monitor conversion recovery after latency improves

## Related Event Patterns

- event_type: click
- repeated click behavior
- high interaction values
- anomaly_flag: -1