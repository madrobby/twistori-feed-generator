require 'sqlite3'
require 'json'

# note: terms must be properly SQL escaped
TERMS = %w(love hate think believe feel wish)
STOPWORDS = %w(# http:// https:// nigger twistori twistory).map(&:downcase)

TERMS.each do |item|
  term = "i #{item}"
  search = "%#{term.downcase}%"

  db = SQLite3::Database.new("db.sqlite")
  query = "SELECT text FROM POST WHERE TEXT LIKE \"#{search}\" AND length(TEXT)<=160 ORDER BY indexedAt DESC LIMIT 200"
  # puts query
  hits = db.execute(query)

  hits = hits.reject do |hit|
    text = hit[0].downcase

    STOPWORDS.any? { |word| text.include? word } ||
    # reject if any of the other "i xxxx" combinations are included,
    # this prevents duplicates between streams
    (TERMS-[item]).any? { |word| text.include? "i #{word}" }
  end

  puts "#{hits.count} hits for #{term}"

  json = "data/#{term.gsub(' ','_')}.json"
  File.open(json,'w'){|f| f.write hits.map{|t|t[0].gsub(/\s+/,' ').strip}.to_json }
end