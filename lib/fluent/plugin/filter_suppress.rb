require 'fluent/plugin/filter'

module Fluent::Plugin
  class SuppressFilter < Fluent::Plugin::Filter
    Fluent::Plugin.register_filter('suppress', self)

    config_param :attr_keys,     :string,  default: nil
    config_param :num,           :integer, default: 3
    config_param :max_slot_num,  :integer, default: 100000
    config_param :interval,      :integer, default: 300

    def configure(conf)
      super
      @keys  = @attr_keys ? @attr_keys.split(/ *, */) : nil
      @slots = {}
    end

    def filter(tag, time, record)
      if @keys
        keys = @keys.map do |key|
          keyArray = key.split(/\./)

          #loops trough the nested record for k as in the key.
          keyArray.inject(record) {|r, k|
            # If the record does not contain the expected key, it does not match what we expect and should not be suppressed.
            if r!=nil and r.key?(k)
              r[k]
            else
              return record
            end
          }
        end
        key = tag + "\0" + keys.join("\0")
      else
        key = tag
      end
      slot = @slots[key] ||= []

      # expire old records time
      expired = time.to_f - @interval
      while slot.first && (slot.first <= expired)
        slot.shift
      end

      if slot.length >= @num
        log.debug "suppressed record: #{record.to_json}"
        return nil
      end

      if @slots.length > @max_slot_num
        (evict_key, evict_slot) = @slots.shift
        if evict_slot.last && (evict_slot.last > expired)
          log.warn "@slots length exceeded @max_slot_num: #{@max_slot_num}. Evicted slot for the key: #{evict_key}"
        end
      end

      slot.push(time.to_f)
      return record
    end
  end
end
