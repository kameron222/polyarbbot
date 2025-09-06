#!/usr/bin/env python3
"""
Strict Market Matcher
Ultra-high quality matching with very strict filters to eliminate false positives
"""

import json
import re
import datetime as dt
from typing import Dict, List, Tuple, Optional, Set
from collections import defaultdict

try:
    from rapidfuzz import fuzz, process, utils as rf_utils
except ImportError:
    raise SystemExit("Install rapidfuzz: pip install rapidfuzz")


def load_and_preprocess(kalshi_path: str, poly_path: str) -> Tuple[List[Dict], List[Dict]]:
    """Load and preprocess both market datasets"""
    print("Loading market data...")
    
    with open(kalshi_path) as f:
        kalshi_raw = json.load(f)
    with open(poly_path) as f:
        poly_raw = json.load(f)
    
    print(f"Raw: {len(kalshi_raw)} Kalshi, {len(poly_raw)} Polymarket")
    
    # Process Kalshi markets
    kalshi = []
    for i, m in enumerate(kalshi_raw):
        title = m.get('title', '').strip()
        desc = m.get('description', '').strip()
        if not title:
            continue
            
        # Combine title and description
        text = f"{title}. {desc}".strip()
        
        # Parse end date
        end_dt = None
        end_date = m.get('endDate') or m.get('close_time')
        if end_date:
            try:
                end_dt = dt.datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            except:
                pass
        
        # Extract key entities and numbers with strict rules
        entities = extract_entities_strict(text)
        numbers = extract_numbers_strict(text)
        
        # Classify domain
        domain = classify_domain_strict(text)
        
        kalshi.append({
            'idx': i,
            'id': m.get('conditionId', ''),
            'title': title,
            'text': text,
            'text_norm': rf_utils.default_process(text),
            'end_dt': end_dt,
            'numbers': numbers,
            'entities': entities,
            'domain': domain,
            'prices': {
                'bid': m.get('bestBid'),
                'ask': m.get('bestAsk')
            }
        })
    
    # Process Polymarket markets
    poly = []
    for i, m in enumerate(poly_raw):
        question = m.get('question', '').strip()
        desc = m.get('description', '').strip()
        if not question:
            continue
            
        text = f"{question}. {desc}".strip()
        
        # Parse end date
        end_dt = None
        end_date = m.get('endDate')
        if end_date:
            try:
                end_dt = dt.datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            except:
                pass
        
        # Parse outcomes
        outcomes = m.get('outcomes', [])
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except:
                outcomes = []
        
        entities = extract_entities_strict(text)
        numbers = extract_numbers_strict(text)
        domain = classify_domain_strict(text)
        
        poly.append({
            'idx': i,
            'id': m.get('id', ''),
            'title': question,
            'text': text,
            'text_norm': rf_utils.default_process(text),
            'end_dt': end_dt,
            'numbers': numbers,
            'entities': entities,
            'domain': domain,
            'outcomes': outcomes
        })
    
    print(f"Processed: {len(kalshi)} Kalshi, {len(poly)} Polymarket")
    return kalshi, poly


def extract_entities_strict(text: str) -> Set[str]:
    """Extract key entities with strict word boundary matching"""
    entities = set()
    text_lower = text.lower()
    
    # Use word boundaries to avoid partial matches
    def has_entity(pattern: str) -> bool:
        return bool(re.search(rf'\b{re.escape(pattern)}\b', text_lower))
    
    # People (exact matches only)
    people_patterns = [
        ('trump', r'\b(donald\s+)?trump\b'),
        ('biden', r'\b(joe\s+)?biden\b'),
        ('harris', r'\b(kamala\s+)?harris\b'),
        ('musk', r'\b(elon\s+)?musk\b'),
        ('putin', r'\b(vladimir\s+)?putin\b'),
        ('xi jinping', r'\bxi\s+jinping\b'),
        ('taylor swift', r'\btaylor\s+swift\b'),
        ('netanyahu', r'\bnetanyahu\b')
    ]
    
    for entity, pattern in people_patterns:
        if re.search(pattern, text_lower):
            entities.add(entity)
    
    # Crypto (strict word boundaries)
    crypto_patterns = [
        ('bitcoin', r'\bbitcoin\b'),
        ('btc', r'\bbtc\b'),
        ('ethereum', r'\bethereum\b'),
        ('eth', r'\beth\b(?!\s*(flipped|flip))'),  # Avoid "eth flipped"
        ('solana', r'\bsolana\b'),
        ('sol', r'\bsol\b(?!\s*\w)'),  # SOL as standalone word only
        ('dogecoin', r'\bdogecoin\b'),
        ('doge', r'\bdoge\b')
    ]
    
    for entity, pattern in crypto_patterns:
        if re.search(pattern, text_lower):
            entities.add(entity)
    
    # Organizations
    org_patterns = [
        ('federal reserve', r'\bfederal\s+reserve\b'),
        ('fed', r'\bfed\b(?!\s*(cup|ex))'),  # Fed but not FedEx or Fed Cup
        ('openai', r'\bopenai\b'),
        ('tesla', r'\btesla\b'),
        ('apple', r'\bapple\b(?!\s*(music|tv))'),
        ('microsoft', r'\bmicrosoft\b'),
        ('google', r'\bgoogle\b'),
        ('meta', r'\bmeta\b(?!\s*\w)'),  # Meta as standalone
        ('netflix', r'\bnetflix\b')
    ]
    
    for entity, pattern in org_patterns:
        if re.search(pattern, text_lower):
            entities.add(entity)
    
    # Countries/Places (exact matches)
    places = [
        ('usa', r'\b(usa|united\s+states|america)\b'),
        ('china', r'\bchina\b'),
        ('russia', r'\brussia\b'),
        ('ukraine', r'\bukraine\b'),
        ('israel', r'\bisrael\b'),
        ('iran', r'\biran\b'),
        ('germany', r'\bgermany\b'),
        ('france', r'\bfrance\b'),
        ('netherlands', r'\bnetherlands\b'),
        ('norway', r'\bnorway\b')
    ]
    
    for entity, pattern in places:
        if re.search(pattern, text_lower):
            entities.add(entity)
    
    # Events/Concepts
    events = [
        ('election', r'\belection\b'),
        ('recession', r'\brecession\b'),
        ('inflation', r'\binflation\b'),
        ('unemployment', r'\bunemployment\b'),
        ('interest rate', r'\binterest\s+rate\b')
    ]
    
    for entity, pattern in events:
        if re.search(pattern, text_lower):
            entities.add(entity)
    
    return entities


def extract_numbers_strict(text: str) -> Set[str]:
    """Extract meaningful numbers with context"""
    numbers = set()
    
    # Years
    years = re.findall(r'\b(202[0-9])\b', text)
    numbers.update(years)
    
    # Percentages
    percentages = re.findall(r'\b(\d+(?:\.\d+)?)\s*%', text)
    numbers.update(percentages)
    
    # Dollar amounts
    dollars = re.findall(r'\$(\d+(?:,\d+)*(?:\.\d+)?)\s*([kmb]?)', text.lower())
    for amount, suffix in dollars:
        numbers.add(f"${amount}{suffix}")
    
    # Basis points
    bps = re.findall(r'\b(\d+)\s*bps?\b', text.lower())
    for bp in bps:
        numbers.add(f"{bp}bps")
    
    # Price levels
    prices = re.findall(r'\b(\d+(?:,\d+)*(?:\.\d+)?)\b', text)
    for price in prices:
        # Only keep significant numbers (not tiny decimals or single digits)
        try:
            val = float(price.replace(',', ''))
            if val >= 10:  # Significant numbers only
                numbers.add(price)
        except:
            pass
    
    return numbers


def classify_domain_strict(text: str) -> str:
    """Strict domain classification"""
    t = text.lower()
    
    # Politics - must have political keywords
    if re.search(r'\b(election|president|presidential|trump|biden|harris|mayor|governor|senate|congress|vote|political|party|democrat|republican|prime minister)\b', t):
        return 'politics'
    
    # Macro - monetary policy specific
    if re.search(r'\b(federal reserve|fomc|fed|interest rate|inflation|unemployment|gdp|recession|monetary policy|basis points|bps)\b', t):
        return 'macro'
    
    # Crypto - digital assets
    if re.search(r'\b(bitcoin|btc|ethereum|eth|crypto|blockchain|solana|sol|dogecoin|doge|defi|nft)\b', t):
        return 'crypto'
    
    # Finance - markets and stocks
    if re.search(r'\b(s&p|spx|nasdaq|dow|stock market|index|tesla|apple|microsoft|amazon|earnings|revenue|market cap)\b', t):
        return 'finance'
    
    # Tech - technology
    if re.search(r'\b(openai|gpt|ai|artificial intelligence|iphone|android|app|software|tech|google|apple|microsoft)\b', t):
        return 'tech'
    
    # Sports
    if re.search(r'\b(nfl|nba|mlb|nhl|soccer|football|basketball|baseball|hockey|championship|super bowl|world cup|olympics)\b', t):
        return 'sports'
    
    # Entertainment
    if re.search(r'\b(taylor swift|album|billboard|rotten tomatoes|movie|oscar|grammy|netflix|box office|streaming)\b', t):
        return 'entertainment'
    
    return 'other'


def semantic_similarity_check(k_text: str, p_text: str) -> bool:
    """Check if markets are semantically similar (not opposite)"""
    k_lower = k_text.lower()
    p_lower = p_text.lower()
    
    # Check for opposite meanings
    opposite_pairs = [
        ('above', 'below'),
        ('over', 'under'),
        ('more than', 'less than'),
        ('increase', 'decrease'),
        ('rise', 'fall'),
        ('up', 'down'),
        ('win', 'lose'),
        ('outperform', 'underperform'),
        ('cut', 'hike'),
        ('emergency', 'scheduled')
    ]
    
    for word1, word2 in opposite_pairs:
        if (word1 in k_lower and word2 in p_lower) or (word2 in k_lower and word1 in p_lower):
            return False
    
    # Check for conflicting numbers (different amounts)
    k_bps = re.findall(r'(\d+)\s*bps?', k_lower)
    p_bps = re.findall(r'(\d+)\s*bps?', p_lower)
    
    if k_bps and p_bps:
        k_amounts = set(k_bps)
        p_amounts = set(p_bps)
        if not (k_amounts & p_amounts):  # No overlap in amounts
            return False
    
    return True


def is_high_quality_match(k: Dict, p: Dict, score: float) -> bool:
    """Ultra-strict quality check"""
    
    # Very high minimum similarity
    if score < 80:
        return False
    
    # Must have substantial entity overlap
    entity_intersection = k['entities'] & p['entities']
    if not entity_intersection:
        return False
    
    # Entity overlap must be significant
    entity_union = k['entities'] | p['entities']
    entity_overlap_ratio = len(entity_intersection) / len(entity_union) if entity_union else 0
    if entity_overlap_ratio < 0.3:
        return False
    
    # Semantic similarity check
    if not semantic_similarity_check(k['text'], p['text']):
        return False
    
    # Domain-specific strict checks
    if k['domain'] == 'politics':
        # Must share specific political entities
        political_entities = {'trump', 'biden', 'harris', 'election', 'president'}
        if not (k['entities'] & p['entities'] & political_entities):
            return False
    
    elif k['domain'] == 'crypto':
        # Must share same cryptocurrency
        crypto_entities = {'bitcoin', 'btc', 'ethereum', 'eth', 'solana', 'sol', 'dogecoin', 'doge'}
        shared_crypto = k['entities'] & p['entities'] & crypto_entities
        if not shared_crypto:
            return False
    
    elif k['domain'] == 'macro':
        # Must share Fed/economic entities
        macro_entities = {'federal reserve', 'fed', 'interest rate', 'unemployment', 'inflation'}
        if not (k['entities'] & p['entities'] & macro_entities):
            return False
    
    # If both have numbers, they should be related
    if k['numbers'] and p['numbers']:
        # For rate/percentage markets, numbers should overlap or be close
        if not (k['numbers'] & p['numbers']):
            # Check if they're close (within reasonable range)
            k_vals = set()
            p_vals = set()
            
            for num in k['numbers']:
                try:
                    if 'bps' in num:
                        k_vals.add(int(num.replace('bps', '')))
                    elif '%' in num:
                        k_vals.add(float(num.replace('%', '')))
                except:
                    pass
            
            for num in p['numbers']:
                try:
                    if 'bps' in num:
                        p_vals.add(int(num.replace('bps', '')))
                    elif '%' in num:
                        p_vals.add(float(num.replace('%', '')))
                except:
                    pass
            
            # Check if any numbers are close (within 50 bps or 1%)
            close_match = False
            for k_val in k_vals:
                for p_val in p_vals:
                    if abs(k_val - p_val) <= max(50, k_val * 0.2):  # Within 50 or 20%
                        close_match = True
                        break
                if close_match:
                    break
            
            if not close_match and score < 95:
                return False
    
    return True


def find_matches(kalshi: List[Dict], poly: List[Dict], 
                threshold: int = 80, max_time_diff_hours: int = 24) -> List[Dict]:
    """Find ultra-high-quality matching markets"""
    
    print(f"Finding strict quality matches with threshold {threshold}, max time diff {max_time_diff_hours}h...")
    
    # Group by domain only (tighter time filtering)
    poly_by_domain = defaultdict(list)
    for p in poly:
        poly_by_domain[p['domain']].append(p)
    
    matches = []
    processed = 0
    
    for k in kalshi:
        processed += 1
        if processed % 3000 == 0:
            print(f"  Processed {processed}/{len(kalshi)} Kalshi markets...")
        
        k_domain = k['domain']
        
        # Only check same domain
        candidates = poly_by_domain.get(k_domain, [])
        if not candidates:
            continue
        
        # Pre-filter by time if both have timestamps
        if k['end_dt']:
            time_filtered = []
            for p in candidates:
                if p['end_dt']:
                    time_diff = abs((k['end_dt'] - p['end_dt']).total_seconds()) / 3600
                    if time_diff <= max_time_diff_hours:
                        time_filtered.append(p)
                else:
                    time_filtered.append(p)  # Include unknown times
            candidates = time_filtered
        
        if not candidates:
            continue
        
        # Prepare texts for RapidFuzz
        candidate_texts = [c['text_norm'] for c in candidates]
        
        # Find top matches
        try:
            results = process.extract(
                k['text_norm'],
                candidate_texts,
                scorer=fuzz.token_set_ratio,
                score_cutoff=threshold,
                limit=1  # Only best match
            )
        except:
            continue
        
        # Process results with ultra-strict quality checks
        for text, score, idx in results:
            p = candidates[idx]
            
            # Ultra-strict quality filter
            if not is_high_quality_match(k, p, score):
                continue
            
            # Final time check
            time_diff = None
            if k['end_dt'] and p['end_dt']:
                time_diff = abs((k['end_dt'] - p['end_dt']).total_seconds()) / 3600
                if time_diff > max_time_diff_hours:
                    continue
            
            # Calculate overlap scores
            entity_intersection = k['entities'] & p['entities']
            entity_union = k['entities'] | p['entities']
            entity_score = len(entity_intersection) / len(entity_union) if entity_union else 0
            
            number_intersection = k['numbers'] & p['numbers']
            number_union = k['numbers'] | p['numbers']
            number_score = len(number_intersection) / len(number_union) if number_union else 0
            
            matches.append({
                'kalshi_idx': k['idx'],
                'poly_idx': p['idx'],
                'kalshi_id': k['id'],
                'poly_id': p['id'],
                'kalshi_title': k['title'],
                'poly_title': p['title'],
                'score': score,
                'domain': k_domain,
                'time_diff_hours': round(time_diff, 1) if time_diff else None,
                'entity_overlap': round(entity_score, 3),
                'number_overlap': round(number_score, 3),
                'shared_entities': sorted(list(entity_intersection)),
                'shared_numbers': sorted(list(number_intersection))
            })
    
    print(f"Found {len(matches)} strict quality candidate matches")
    return matches


def deduplicate_matches(matches: List[Dict]) -> List[Dict]:
    """Remove duplicates with ultra-strict 1-1 matching"""
    print("Deduplicating matches...")
    
    # Sort by composite quality score
    def quality_score(m):
        return m['score'] + (m['entity_overlap'] * 30) + (m['number_overlap'] * 20)
    
    matches.sort(key=quality_score, reverse=True)
    
    used_kalshi = set()
    used_poly = set()
    final_matches = []
    
    for match in matches:
        k_idx = match['kalshi_idx']
        p_idx = match['poly_idx']
        
        if k_idx not in used_kalshi and p_idx not in used_poly:
            used_kalshi.add(k_idx)
            used_poly.add(p_idx)
            final_matches.append(match)
    
    print(f"Final matches after strict deduplication: {len(final_matches)}")
    return final_matches


def save_matches(matches: List[Dict], output_file: str):
    """Save matches to JSON file"""
    output = {
        'generated_at': dt.datetime.now(dt.timezone.utc).isoformat(),
        'total_matches': len(matches),
        'matching_criteria': {
            'min_text_similarity': 80,
            'min_entity_overlap_ratio': 0.3,
            'strict_entity_matching': True,
            'semantic_opposite_filtering': True,
            'domain_exact_match': True,
            'max_time_diff_hours': 24
        },
        'matches': matches
    }
    
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"Saved {len(matches)} ultra-quality matches to {output_file}")


def print_summary(matches: List[Dict]):
    """Print detailed match summary"""
    if not matches:
        print("No matches found!")
        return
    
    print(f"\n{'='*80}")
    print(f"ULTRA-STRICT MATCH SUMMARY: {len(matches)} total matches")
    print(f"{'='*80}")
    
    # Group by domain
    by_domain = defaultdict(int)
    for m in matches:
        by_domain[m['domain']] += 1
    
    print("By domain:")
    for domain, count in sorted(by_domain.items(), key=lambda x: x[1], reverse=True):
        print(f"  {domain}: {count}")
    
    # Quality metrics
    scores = [m['score'] for m in matches]
    entity_scores = [m['entity_overlap'] for m in matches if m['entity_overlap'] > 0]
    
    print(f"\nQuality metrics:")
    print(f"  Text similarity - Min: {min(scores):.1f}, Max: {max(scores):.1f}, Avg: {sum(scores)/len(scores):.1f}")
    if entity_scores:
        print(f"  Entity overlap - Min: {min(entity_scores):.3f}, Max: {max(entity_scores):.3f}, Avg: {sum(entity_scores)/len(entity_scores):.3f}")
    
    # Top matches with shared entities
    print(f"\nTop 15 ultra-quality matches:")
    for i, m in enumerate(matches[:15], 1):
        print(f"\n{i}. Score: {m['score']:.1f} | Domain: {m['domain']} | Entity: {m['entity_overlap']:.3f}")
        print(f"   Kalshi: {m['kalshi_title']}")
        print(f"   Poly:   {m['poly_title']}")
        if m['shared_entities']:
            print(f"   Shared: {', '.join(m['shared_entities'])}")


def main():
    """Main execution"""
    kalshi_file = "kalshi_markets.json"
    poly_file = "polymarket_current_active_gamma.json"
    output_file = "strict_matches.json"
    
    # Load and preprocess
    kalshi, poly = load_and_preprocess(kalshi_file, poly_file)
    
    if len(kalshi) == 0 or len(poly) == 0:
        print("Error: No markets loaded!")
        return
    
    # Find ultra-strict matches
    print(f"\nFinding ultra-strict matches...")
    matches = find_matches(kalshi, poly, threshold=80, max_time_diff_hours=24)
    final_matches = deduplicate_matches(matches)
    
    save_matches(final_matches, output_file)
    print_summary(final_matches)


if __name__ == "__main__":
    main()
