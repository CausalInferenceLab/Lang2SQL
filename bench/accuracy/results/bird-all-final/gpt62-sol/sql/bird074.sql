SELECT COUNT(DISTINCT name) AS release_promo_card_count
FROM cards
WHERE isPromo = 1
  AND instr(',' || promoTypes || ',', ',release,') > 0;
