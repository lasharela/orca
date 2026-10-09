/**
 * Whether a `window.open()` asks for a new tab rather than a popup window.
 *
 * Orca answers a tab by denying, which hands the page `null`, so named and featured opens — whose
 * flow may use that handle — stay popups. `noopener` and `noreferrer` are not window features: they
 * only cut the opener, so `window.open(url, '_blank', 'noopener,noreferrer')` is still a new tab.
 */
export function isNewBrowserTabPopupIntent(details: {
  frameName: string
  disposition: string
  features: string
}): boolean {
  return (
    details.frameName === '' &&
    hasOnlyOpenerFeatures(details.features) &&
    (details.disposition === 'foreground-tab' || details.disposition === 'background-tab')
  )
}

function hasOnlyOpenerFeatures(features: string): boolean {
  return features
    .split(/[\s,]+/)
    .every((feature) => feature === '' || /^no(?:opener|referrer)(?:=.*)?$/i.test(feature))
}
