from jobs.basic.get_serp import EntitySerpDaily
from jobs.basic.get_suggest import EntitySuggestDaily

# test = EntitySerpDaily("2024111116", "ko", "google", "False")
test = EntitySuggestDaily("ko", "google")
test.test()